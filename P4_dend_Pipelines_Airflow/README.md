# Data Pipeline with Airflow

## Overview

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

## Project Description

This project creates a high grade data pipeline that is dynamic and built from reusable tasks, can be monitored, and allow easy backfills. As data quality plays a big part when analyses are executed on top the data warehouse, thus the pipeline runs tests against Sparkify's datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Datasets
Here are the s3 links for datasets used in this project:

`Log data: s3://udacity-dend/log_data`
`Song data: s3://udacity-dend/song_data`

## Airflow Structure
- `sparkify_etl_dag.py`: Defines main DAG, tasks and link the tasks in required order.
- `stage_redshift.py`:copy JSON data from S3 to staging tables. (StageToRedshiftOperator)
- `load_dimension.py`: load a dimension table from staging table(s). (LoadDimensionOperator)
- `load_fact.py`:  load fact table from staging tables. (LoadFactOperator)
- `data_quality.py`: run data quality checks on all tables passed as parameter. (DataQualityOperator)

![Airflow DAG](dag_flow.png)

## Steps to run Program
- Create a Redshift cluster
- Run `create_tables.sql`.
- Setup two Airflow connections:
    - AWS credentials, named `aws_credentials`
    - Connection to Redshift, named `redshift`
