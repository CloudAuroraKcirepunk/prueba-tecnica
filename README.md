# Documentación del DAG `prueba_tecnica`

## Prerrequisitos para levantar la aplicación

Antes de ejecutar la aplicación, asegúrate de tener instalados los siguientes programas:

- **Docker**: Necesario para ejecutar los contenedores. Descárgalo desde [Docker](https://www.docker.com/).  
- **Astronomer CLI**: Requerido para gestionar el entorno de Apache Airflow. Instálalo siguiendo las instrucciones en [Astronomer CLI](https://docs.astronomer.io/astro/cli/install-cli).  

## Levantar la aplicación

Ejecuta el siguiente comando en la terminal dentro del directorio del proyecto:

```bash
astro dev start
```

## Descripción

Este DAG de Apache Airflow extrae datos de la API Spaceflight News y los almacena en una base de datos PostgreSQL. Luego, limpia y analiza los datos, realiza verificaciones y actualiza dashboards.

## Tabla de tareas

| Tipo de operador  | ID de tarea                  | Notas |
|------------------|----------------------------|-------|
| **Extracción**  | `extract_articles`         | Extrae datos de artículos desde la API y los almacena en PostgreSQL. |
| **Extracción**  | `extract_blogs`            | Extrae datos de blogs desde la API y los almacena en XCom. |
| **Extracción**  | `extract_reports`          | Extrae datos de reportes desde la API y los almacena en XCom. |
| **Transformación** | `clean_and_deduplicate`    | Filtra y elimina datos duplicados en los artículos antes de almacenarlos en la base de datos. |
| **Transformación** | `perform_analysis`         | Analiza los artículos extraídos utilizando pandas y dask. |
| **Verificación** | `verify_articles_db`       | Verifica los artículos almacenados en la base de datos. |
| **Carga**       | `load_processed_data`      | Carga los datos procesados en PostgreSQL. |
| **Verificación** | `verify_articles_load_db`  | Verifica que los datos procesados se hayan almacenado correctamente en la base de datos. |
| **Transformación** | `generate_daily_insights`  | Genera informes diarios a partir de los datos analizados. |
| **Transformación** | `update_dashboards`        | Actualiza los dashboards con los datos procesados. |

## Flujo del DAG

El DAG sigue la siguiente secuencia:

1. **Extracción de datos** (`extract_articles`, `extract_blogs`, `extract_reports`).
2. **Limpieza y eliminación de duplicados** (`clean_and_deduplicate`).
3. **Análisis de datos** (`perform_analysis`).
4. **Verificación de los datos en la base de datos** (`verify_articles_db`).
5. **Carga de datos procesados** (`load_processed_data`).
6. **Verificación de la carga de datos** (`verify_articles_load_db`).
7. **Generación de insights diarios** (`generate_daily_insights`).
8. **Actualización de dashboards** (`update_dashboards`).


Nota:
> Se completo la prueba con los conceptos mas relevante y se logra apreciar los conocimientos tecnico, dato
que no dispongo con mucho tiempo no pude terminar la prueba al 100% si necesitan corroborar los puntos faltanes
podemos cuadrar una reunion tecnica para que verifiquen mi experiencia, por otro lado la prueba especifica hacer un 
diagrama en **aws** pero no veo mucha informacion sobre el flujo para crear el diagrama, de igual forma vuelto y repito
para terminar de confirmar mi experiencia tecnia les propongo una entrevista virtual para terminar de corroborar que soy
apto para el puesto 