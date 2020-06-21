# Sparkfy AWS Data Warehouse Project

## Motivation

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, I have beem tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables that the analytics team can use for discovering insights into what songs their users are listening to.

## The Datasets

1. Songs dataset: JSONs files that contain metadata about a song and the artist of that song.
2. Log dataset:   log files in JSON format based on events simulated from the songs in the songs dataset above.

## Project structure

```
redshift_data_warehouse
|__ infrastructure
|   |__ __init__.py
|   |__ delete_cluster.py
|   |__ provision_cluster.py
|__  .gitignore
|__ create_tables.py
|__ etl.py
|__ README.md
|__ requirements.txt
|__ sql_queries.py
```

## Instructions to Run

Make sure AWS cluster configuraitons are contained in a dwh.cfg file in the parent directory

Also, you'll need to create a virtual environment and install the requirements using the `requirements.txt` file.

1. Provision AWS Redshift cluster
```
  python infrastructure/provision_cluster.py`
```
2. Create the schemas for the staging and normalized tables

**Note: First verify that the AWS Redshift cluster has been fully provisioned and available for use before running this step**
```
   python create_tables.py`
```
3. Execute the ETL pipeline
```
   python etl.py`
```
4. Delete the Redshift cluster when no longer required
```
   python infrastructure/delete_cluster.py`
```
