# Data Lake with Apache Spark

--------------------------------------------

#### Introduction
Data source in s3 bucket containing log data and songs data.
Using apache spark to orchestrate a data lake pipeline by pulling data from 
s3 public s3 bucket, normalizing it to create distinct tables and pushing the distinct
tables as parquet files back to s3.

--------------------------------------------
#### ETL Process

Using the star schema, normalized user and artist information is extracted from the song data
located in an s3 bucket. This data is processed using spark to set column field types and also
also extract the needed columns. A log data is also used extract information about time, users,
and songplays which is a fact tables. This is also done using spark to filter songs by actions,
set column types and extract the needed columns.

-------------------------
#### Project structure

* <b> etl.py </b> - The script that handles the whole etl orchestration
* <b> dl.cfg </b> - Contains AWS configuration information

## Project Description

In this project, I'll apply what I've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, I will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

![Sparkify Database Schema!](data-model-cut.png)

## USING THE STAR SCHEMA TO MODEL DIMENSIONS AND FACT TABLES FROM STAGING TABLES

This is the simplest way to structure tables in a database. As can be seen above the structure looks like a star. This is a combination of a central fact table surrounded by related dimension tables. 

This is optimized for querying large datasets and it is easy to understand. Dimension tables are linked to the fact table with foreign keys.