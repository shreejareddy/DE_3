# IoT Data Processing Pipeline with AWS EMR

## Overview

This project implements an IoT data processing pipeline using AWS services, including S3, EMR, IAM, and CloudWatch. The script simulates IoT sensor data, uploads it to an S3 bucket, processes the data using a Spark job on an EMR cluster, and sets up CloudWatch alarms to monitor the cluster's health.

## Features

- IAM Roles Creation: Creates the necessary IAM roles and instance profiles for the EMR cluster.
- S3 Buckets: Automatically creates S3 buckets for raw and processed data storage.
- IoT Data Simulation: Generates and uploads random IoT sensor data to S3.
- EMR Cluster Creation: Launches an EMR cluster for processing IoT data using Spark.
- Spark Job: Defines and submits a Spark job to process the IoT data.
- CloudWatch Alarms: Sets up CloudWatch alarms to monitor the EMR cluster's CPU utilization.

## Prerequisites

Before running this script, ensure you have the following:

- Python 3.x installed.
- AWS CLI configured with your credentials and default region.
- Boto3 library installed. You can install it using pip:
  ```bash
  pip install boto3
  pip install boto3 botocore
## Contributions 
-Data Ingestion and S3 Setup
  -Python script to simulate IoT sensor data.
  -Code to stream data to S3 in real time.
  -Configuration of S3 buckets and IAM roles using the AWS SDK.
-Data Processing and EMR Setup
  -Code to set up and configure the EMR cluster.
  -A spark job script has been developed to process the streaming data.
  -Verification that processed data is correctly stored back in S3.
-Monitoring, CloudWatch Setup, and Documentation
  -Set up CloudWatch monitoring for the EMR cluster using the AWS SDK (boto3).
  -Configure CloudWatch metrics and alarms to monitor the performance of the EMR cluster.

