# Project Description
The purpouse of this project is to build a data warehouses with AWS using an ETL pipeline for a database hosted on Redshift. It must load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

# Overview
Using two Amazon Web Services: S3 (Data storage) and Redshift (Data warehouse with columnar storage)

Two public S3 buckets provide the data sources (JSON files). One bucket contains info about songs and artists, the second has info concerning actions done by users. 
The song bucket has all the files under the same directory, while the event bucket does not, so we require a descriptor file in order to extract data from the folders by path (as we don't have a common prefix on folders). 

The Redshift service is where data will be ingested and transformed,

# Schema
![](https://r766469c826419xjupyterlr5tapor7.udacity-student-workspaces.com/files/Modelo_DE_DW_03.PNG?_xsrf=2%7C5087ea5c%7Cc5daf650716354804650c2e1e5a6a885%7C1591143045)