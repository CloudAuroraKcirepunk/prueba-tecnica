import logging
from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime
from urllib.parse import urlencode
import requests
import json
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import dask.dataframe as dd
from dask import delayed
from collections import Counter
import string

# Configuración del logger
logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

ARTICLES_API_URL = "https://api.spaceflightnewsapi.net/v4/articles"
REPORTS_API_URL = "https://api.spaceflightnewsapi.net/v4/reports"
BLOGS_API_URL = "https://api.spaceflightnewsapi.net/v4/blogs"
DEFAULT_LIMIT = 100
DEFAULT_OFFSET = 0

@dag(
    start_date=datetime(2025, 2, 2),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Erick Torres", "retries": 3},
)
def prueba_tecnica():
    @task()
    def extract_articles(**context):
        pg_hook = PostgresHook(postgres_conn_id='postgres')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS fact_article (
                id SERIAL PRIMARY KEY,
                article_id INT,
                title VARCHAR(500),
                url TEXT,
                image_url TEXT,
                news_site VARCHAR(255),
                summary TEXT,
                published_at TIMESTAMP,
                updated_at TIMESTAMP,
                featured BOOLEAN,
                launches JSONB,
                events JSONB
            );
            """)

        ref = context['ti']
        limit = DEFAULT_LIMIT
        offset = DEFAULT_OFFSET
        params = {'limit': limit, 'offset': offset}

        articles = []

        for _ in range(1, 6):
            final_url = f'{ARTICLES_API_URL}?{urlencode(params)}'
            response = requests.get(final_url)
            data = response.json()
            articles.extend(data['results'])
            offset += limit

        ref.xcom_push(key='articles_data', value=articles)

        conn.commit()
        cursor.close()
        conn.close()

        logger.info(f"Extracted {len(articles)} articles.")

    @task()
    def extract_blogs(**context):
        ref = context['ti']
        limit = DEFAULT_LIMIT
        offset = DEFAULT_OFFSET
        params = {'limit': limit, 'offset': offset}

        blogs = []

        for _ in range(1, 6):
            final_url = f'{BLOGS_API_URL}?{urlencode(params)}'
            response = requests.get(final_url)
            data = response.json()
            blogs.extend(data['results'])
            offset += limit

        ref.xcom_push(key='blogs_data', value=blogs)

        logger.info(f"Extracted {len(blogs)} blogs.")

    @task()
    def extract_reports(**context):
        ref = context['ti']
        limit = DEFAULT_LIMIT
        offset = DEFAULT_OFFSET
        params = {'limit': limit, 'offset': offset}

        reports = []

        for _ in range(1, 6):
            final_url = f'{REPORTS_API_URL}?{urlencode(params)}'
            response = requests.get(final_url)
            data = response.json()
            reports.extend(data['results'])
            offset += limit

        ref.xcom_push(key='reports_data', value=reports)

        logger.info(f"Extracted {len(reports)} reports.")

    @task()
    def clean_and_deduplicate(**context):
        ref = context['ti']
        articles = ref.xcom_pull(task_ids='extract_articles', key='articles_data')
        blogs = ref.xcom_pull(task_ids='extract_blogs', key='blogs_data')
        reports = ref.xcom_pull(task_ids='extract_reports', key='reports_data')

        process_data = (articles or []) + (blogs or []) + (reports or [])

        logger.info(f'Articles {len(articles)}')
        logger.info(f'Reports {len(reports)}')
        logger.info(f'Blogs {len(blogs)}')

        pg_hook = PostgresHook(postgres_conn_id='postgres')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        process_ids = list(map(lambda x: x['id'], process_data))
        process_ids_str = ",".join(map(str, process_ids))
        sql_query = f"""
            SELECT article_id
            FROM fact_article
            WHERE article_id IN ({process_ids_str});
        """

        cursor.execute(sql_query)
        articles_data = cursor.fetchall()
        articles_ids_db = {row[0] for row in articles_data}

        process_for_imports = []

        if len(articles_data) > 0:
            process_for_imports = list(filter(lambda x: x['id'] not in articles_ids_db, process_data))
            logger.info(f'Filtered articles: {len(process_for_imports)}')
        else:
            process_for_imports = articles

        ref.xcom_push(key='articles_data_clean', value=process_for_imports)

    @task()
    def perform_analysis(**context):
        ref = context['ti']

        articles = ref.xcom_pull(task_ids='extract_articles', key='articles_data')

        articles_df = pd.DataFrame(articles)

        articles_ddf = dd.from_pandas(articles_df, npartitions=2)

        logger.info(f"Columns of the DataFrame: {articles_ddf.columns}")

        articles_count_by_site = articles_ddf.groupby('news_site').size().compute()

        logger.info(f'Number of articles per news source {articles_count_by_site}')
        
        articles_ddf['published_at'] = dd.to_datetime(articles_ddf['published_at'])

        articles_ddf['year_month'] = articles_ddf['published_at'].dt.to_period('M')

        articles_count_by_month = articles_ddf.groupby(['year_month', 'news_site']).size().compute()

        logger.info(f"Articles trends by month: {articles_count_by_month}")

    @task()
    def verify_articles_db(**context):
        ref = context['ti']

        pg_hook = PostgresHook(postgres_conn_id='postgres')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM fact_article;")
        rows = cursor.fetchall()

        logger.info(f'Current Articles saved in db: {len(rows)}')

        conn.commit()
        cursor.close()
        conn.close()

        ref.xcom_push(key='current_articles_saved_db', value=rows)

    @task()
    def load_processed_data(**context):
        ref = context['ti']

        articles = ref.xcom_pull(task_ids='clean_and_deduplicate', key='articles_data_clean')

        insert_query = """
           INSERT INTO fact_article (article_id, title, url, image_url, news_site, summary, published_at, updated_at, featured, launches, events)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        if len(articles) > 0:
            pg_hook = PostgresHook(postgres_conn_id='postgres')
            conn = pg_hook.get_conn()
            cursor = conn.cursor()

            for article in articles:
                cursor.execute(insert_query, (
                    article["id"],
                    article["title"],
                    article["url"],
                    article["image_url"],
                    article["news_site"],
                    article["summary"],
                    article["published_at"],
                    article["updated_at"],
                    article.get('featured', False),
                    json.dumps(article.get("launches", [])),
                    json.dumps(article.get("events", []))
                ))

            conn.commit()
            cursor.close()
            conn.close()

        logger.info('Loaded processed data into fact_article.')

    @task()
    def verify_articles_load_db(**context):
        ref = context['ti']

        pg_hook = PostgresHook(postgres_conn_id='postgres')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM fact_article;")
        rows = cursor.fetchall()

        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f'Current Articles Loaded in Database: {len(rows)}')

    @task()
    def generate_daily_insights(**context):
        logger.info('Generating daily insights...')

    @task()
    def update_dashboards():
        logger.info('Updating dashboards...')

    [extract_articles(), extract_blogs(), extract_reports()] >> clean_and_deduplicate() >> perform_analysis() >> verify_articles_db() >> load_processed_data() >> verify_articles_load_db() >> generate_daily_insights() >> update_dashboards()

prueba_tecnica()
