# Sparkify - Data Lake

## Purpose
Sparkify is a music streaming service company that is wanting to understand how it's users interact with it's Sparkify application. The purpose of the database is to organize the data in such a way that it is easy, efficient and cost effective to answer questions that various sparkify company stakeholders may have to improve the service. 

## Summary of Database Schema and ETL Pipeline
User data and song data is stored as JSON Lines files on Amazon S3. The ETL pipeline uses an EMR cluster running Spark to read the raw JSON Lines files from S3, then uses PySpark API to transform the raw data into dimension tables and a fact table to form a star schema. These tables are then loaded back to S3 as partitioned parquet files to optimize the amount of storage space needed and to allow for efficient analytical queries in the future. 

## How to run the python scripts
Using an SSH connection to the EMR cluster run "python etl.py"

## Explanation of files
### dl.cfg
Contains AWS Access Key and Secret Access Key used to give access to the S3 datastore.

### etl.py
Runs the etl pipeline used to create the Sparkify data lake. This script has functions for creating a spark session, extracting the songs data, processing songs data and processing log data. The main function of the script calls the processing songs and processing logs functions that runs the ETL pipeline. The processing functions both require the input and output S3 file paths to specify where the functions extract the data from and load the data to. 

### testing.ipynb
Used as a means to run spark locally in a sandbox environment to refine my understanding of the PySpark API by getting immediate feedback.

### data/
Directory used to store a small portion of the songs and logs JSON files. This subset of the larger dataset allowed me to get immediate feedback on my ETL PySpark scripts to ensure my ETL pipeline would be functional on the larger dataset.