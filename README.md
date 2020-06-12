# Sparkfy Data Warehouse Project

Motivation:
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, I have beem tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for the analytics team to continue finding insights in what songs their users are listening to.

The Datasets:
1. Songs Dataset
2. Log Dataset