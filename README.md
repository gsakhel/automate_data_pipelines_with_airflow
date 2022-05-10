# Automate Data Pipelines with Airflow

### Purpose
This project creates the data pipeplines for staging data from S3 onto Redshift and creates a set of fact and dimmension tables.

### Contents
1. main_elt.py       - main DAG
1. stage_redshift.py - creates tables then loads them with data from S3
1. load_fact.py      - creates fact table and inserts data from staging tables
1. load_dimension.py - creates dimension table and inserts data from staging tables
1. data_quality.py   - tests data quality with user supplied test sql and answer
1. sql_queries.py    - contains sql queries used in main_etl.py
