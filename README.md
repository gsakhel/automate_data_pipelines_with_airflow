# Automate Data Pipelines with Airflow

This project creates the data pipeplines for staging data from S3 onto Redshift and creates a set of fact and dimmension tables.


main_elt.py       -  the main DAG
stage_redshift.py - creates tables then loads them with data from S3
load_fact.py      -  creates fact table and inserts data from staging tables
load_dimension.py - creates dimension table and inserts data from staging tables
data_quality.py   - tests data quality with user supplied test sql and answer
sql_queries.py    - contains sql queries used in main_etl.py