Architecture Overview
This document describes a serverless ETL pipeline built on AWS using Glue, Pyspark, and Lambda.

Here's a breakdown of the data flow:

Data Ingestion: A Python script generates mock data and uploads it to a designated S3 bucket.
Event Notification: When new data arrives in the S3 bucket, an event notification is sent to AWS Lambda via AWS EventBridge.
ETL Processing: Triggered by the event notification, a Lambda function initiates an AWS Glue ETL job. This Glue job leverages PySpark for parallel processing to:
Read data from the S3 bucket
Perform transformations on the data
Write the processed data back to a dedicated S3 location
Data Cataloging: AWS Glue crawlers automatically run on the output S3 bucket to register and catalog the processed data.
Notification: Finally, the Lambda function sends notifications (success or failure) about the Glue ETL job via Amazon SNS.
Prerequisites
To set up this ETL pipeline, you'll need:

An AWS account with proper permissions for S3, Lambda, Glue, EventBridge, and SNS.
AWS CLI configured with your AWS credentials.
Python 3.x installed locally.
