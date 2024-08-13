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
