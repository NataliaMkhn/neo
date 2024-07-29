# Airflow + Postgres + Spark

Пример проекта, с использованием docker compose

Airflow состоит из 3 частей:

- airflow-init: нужен для инициализации бд airflow, при старте импортирует переменные из "./airflow/variables.json" и подключения из "./airflow/connections.json"
- airflow-scheduler: бэкенд
- airflow-webserver: фронтенд
    - http://localhost:8786/
    - airflow/airflow

Обрати внимаение, что папка "./airflow" примонтируется внутрь контейнера, так что даги пишем в папке "./airflow/dags"

Три постгри:

- postgres_airflow: техническая бд для работы airflow (localhost:5433)
- postgres_src: система - источник данных (localhost:5434)
- postgres_dwh: система - приёмник данных (localhost:5435)

Данные для баз будут храниться в примонтированных папках "./data". Подключаться через localhost с нужным портом

Помимо этого, есть Jupyter Lab с pyspark

- http://localhost:9999/
- Токен будет в логах контейнера

В Airflow тоже установлен спарк (через ./airflow/Dockerfile)

Удалить созданные контейнеры + сборка + запуск `docker compose down && docker compose build --no-cache && docker compose up`
При запуске нужно дождаться, пока отработает и выключится контейнер airflow-init
