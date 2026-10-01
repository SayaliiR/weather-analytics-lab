Weather Prediction Analytics: San Jose vs Los Angeles

An end-to-end data pipeline that collects daily weather data for two California cities, loads it into Snowflake, transforms it with dbt and visualises it in Tableau.

Pipeline: Open-Meteo API → Airflow (ETL) → Snowflake (RAW) → dbt (models, tests, snapshot) → Snowflake (ANALYTICS) → Tableau


What it does
Pulls the last 60 days of observed weather and a 7-day forecast for San Jose and Los Angeles from the Open-Meteo API every day.
Loads the data into Snowflake using a SQL transaction, so re-running the pipeline never creates duplicates.
Automatically starts a dbt pipeline after each successful load. dbt builds an analytics table, runs data quality tests and records forecast changes in a snapshot.
Feeds a Tableau dashboard that compares the two cities on temperature trends, anomalies, rainfall, dry spells and heating/cooling demand.

Tech stack
Data source - Open-Meteo Forecast API (free, no API key)
Orchestration - Apache Airflow 2.10.1 (Docker)
Data warehouse - Snowflake
Transformation - dbt-core 1.8.7 + dbt-snowflake 1.8.1
Visualisation - Tableau Desktop
