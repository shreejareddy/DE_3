

import os
import boto3
import json
import time
import random
import logging
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, NoRegionError, ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



# Initialize the S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=REGION_NAME
)

# Define bucket names
RAW_DATA_BUCKET = 'iot-sensor-data-bucket'
PROCESSED_DATA_BUCKET = 'iot-processed-data-bucket'

# Create IAM roles and instance profiles
def create_iam_roles():
    try:
        iam_client = boto3.client(
            'iam',
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=REGION_NAME
        )
        
        # Create EMR EC2 Instance Profile Role
        try:
            iam_client.create_role(
                RoleName='EMR_EC2_DefaultRole',
                AssumeRolePolicyDocument=json.dumps({
                    'Version': '2012-10-17',
                    'Statement': [{
                        'Effect': 'Allow',
                        'Principal': {
                            'Service': 'ec2.amazonaws.com'
                        },
                        'Action': 'sts:AssumeRole'
                    }]
                })
            )
            logger.info("Created IAM role EMR_EC2_DefaultRole.")
            iam_client.attach_role_policy(
                RoleName='EMR_EC2_DefaultRole',
                PolicyArn='arn:aws:iam::aws:policy/service-role/AmazonEC2RoleforEMR'
            )
            logger.info("Attached policy to EMR_EC2_DefaultRole.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityAlreadyExists':
                logger.info("IAM role EMR_EC2_DefaultRole already exists.")
            else:
                raise e

        try:
            iam_client.create_instance_profile(InstanceProfileName='EMR_EC2_DefaultInstanceProfile')
            logger.info("Created instance profile EMR_EC2_DefaultInstanceProfile.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityAlreadyExists':
                logger.info("Instance profile EMR_EC2_DefaultInstanceProfile already exists.")
            else:
                raise e

        iam_client.add_role_to_instance_profile(
            InstanceProfileName='EMR_EC2_DefaultInstanceProfile',
            RoleName='EMR_EC2_DefaultRole'
        )
        logger.info("Added role EMR_EC2_DefaultRole to instance profile EMR_EC2_DefaultInstanceProfile.")

        # Create EMR Service Role
        try:
            iam_client.create_role(
                RoleName='EMR_DefaultRole',
                AssumeRolePolicyDocument=json.dumps({
                    'Version': '2012-10-17',
                    'Statement': [{
                        'Effect': 'Allow',
                        'Principal': {
                            'Service': 'elasticmapreduce.amazonaws.com'
                        },
                        'Action': 'sts:AssumeRole'
                    }]
                })
            )
            logger.info("Created IAM role EMR_DefaultRole.")
            iam_client.attach_role_policy(
                RoleName='EMR_DefaultRole',
                PolicyArn='arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole'
            )
            logger.info("Attached policy to EMR_DefaultRole.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityAlreadyExists':
                logger.info("IAM role EMR_DefaultRole already exists.")
            else:
                raise e

    except Exception as e:
        logger.error(f"Error creating IAM roles: {e}")

# Create the S3 buckets
def create_buckets():
    try:
        s3.create_bucket(Bucket=RAW_DATA_BUCKET, CreateBucketConfiguration={'LocationConstraint': REGION_NAME})
        s3.create_bucket(Bucket=PROCESSED_DATA_BUCKET, CreateBucketConfiguration={'LocationConstraint': REGION_NAME})
        logger.info(f"Buckets '{RAW_DATA_BUCKET}' and '{PROCESSED_DATA_BUCKET}' created.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            logger.info("Buckets already exist.")
        else:
            logger.error(f"Error creating buckets: {e}")

# Simulate IoT sensor data and upload to S3
def simulate_iot_data():
    def generate_sensor_data():
        return {
            'sensor_id': random.randint(1, 1000),
            'temperature': random.uniform(10.0, 50.0),
            'humidity': random.uniform(30.0, 70.0),
            'timestamp': int(time.time())
        }

    for i in range(1):  # Generating 10 sensor data entries for demonstration
        data = generate_sensor_data()
        file_name = f"data_{data['timestamp']}.json"
        try:
            s3.put_object(Bucket=RAW_DATA_BUCKET, Key=file_name, Body=json.dumps(data))
            logger.info(f"Uploaded {file_name} to {RAW_DATA_BUCKET}")
        except Exception as e:
            logger.error(f"Error uploading data: {e}")
        time.sleep(5)  # Simulate data being sent every 5 seconds

def create_emr_cluster():
    try:
        emr_client = boto3.client(
            'emr',
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=REGION_NAME
        )

        # Wait for a few seconds to allow IAM roles and instance profiles to propagate
        logger.info("Waiting for IAM roles and instance profiles to propagate...")
        time.sleep(20)

        response = emr_client.run_job_flow(
            Name='IoT-Data-Processing-Cluster',
            LogUri=f's3://{PROCESSED_DATA_BUCKET}/logs/',
            ReleaseLabel='emr-6.3.0',
            Instances={
                'InstanceGroups': [
                    {
                        'Name': 'Master nodes',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 1,
                    },
                    {
                        'Name': 'Core nodes',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 2,
                    },
                ],
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': True,
            },
            Applications=[
                {'Name': 'Spark'},
            ],
            Configurations=[
                {
                    'Classification': 'spark-env',
                    'Properties': {},
                    'Configurations': [
                        {
                            'Classification': 'export',
                            'Properties': {
                                'PYSPARK_PYTHON': '/usr/bin/python3'
                            }
                        }
                    ]
                }
            ],
            JobFlowRole='EMR_EC2_DefaultInstanceProfile',
            ServiceRole='EMR_DefaultRole',
            AutoTerminationPolicy={
                'IdleTimeout': 3600  # Idle timeout in seconds (e.g., 1 hour)
            },
        )

        cluster_id = response['JobFlowId']
        logger.info(f"Created EMR cluster {cluster_id}")
        return cluster_id

    except NoCredentialsError:
        logger.error("Error: No AWS credentials found.")
    except PartialCredentialsError:
        logger.error("Error: Incomplete AWS credentials.")
    except NoRegionError:
        logger.error("Error: No region specified.")
    except ClientError as e:
        logger.error(f"ClientError: {e}")
    except Exception as e:
        logger.error(f"Error creating EMR cluster: {e}")

# Submit Spark job to EMR
def submit_spark_job(cluster_id):
    try:
        emr_client = boto3.client(
            'emr',
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=REGION_NAME
        )

        step_args = [
            'spark-submit',
            '--deploy-mode', 'cluster',
            f's3://{RAW_DATA_BUCKET}/spark-job.py',
        ]

        step = {
            'Name': 'Spark job for IoT data',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': step_args,
            }
        }

        response = emr_client.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[step]
        )

        step_id = response['StepIds'][0]
        logger.info(f"Added step {step_id} to EMR cluster {cluster_id}")

    except NoCredentialsError:
        logger.error("Error: No AWS credentials found.")
    except PartialCredentialsError:
        logger.error("Error: Incomplete AWS credentials.")
    except NoRegionError:
        logger.error("Error: No region specified.")
    except ClientError as e:
        logger.error(f"ClientError: {e}")
    except Exception as e:
        logger.error(f"Error submitting Spark job: {e}")

# Create Spark job script and upload to S3
def create_spark_job_script():
    try:
        spark_script = f"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, avg, max, min

spark = SparkSession.builder.appName("IoTDataProcessing")
  .config("spark.network.timeout", "600s")
  .config("spark.executor.heartbeatInterval", "100s")
  .config("spark.hadoop.fs.s3a.connection.timeout", "500000")
  .config("spark.hadoop.fs.s3a.connection.maximum", "1000")
  .config("spark.hadoop.fs.s3a.threads.max", "200")
  .config("spark.hadoop.fs.s3a.retry.limit", "10")
  .config("spark.hadoop.fs.s3a.attempts.maximum", "10")
  .config("spark.hadoop.fs.s3a.retry.interval", "2000ms")
  .getOrCreate()

# Read data from S3
input_path = "s3://{RAW_DATA_BUCKET}/data*.jso"

df = spark.read.json(input_path)
df.show()

# Data transformation
df = df.filter((col('temperature') > 0) & (col('temperature') < 50))
df = df.withColumn('timestamp', from_unixtime(col('timestamp')))
df = df.groupBy("sensor_id").agg(
    avg("temperature").alias("avg_temperature"),
    max("humidity").alias("max_humidity"),
    min("temperature").alias("min_temperature")
)
df.show()

# Write the result back to S3
output_path = "s3://{PROCESSED_DATA_BUCKET}/"
df.write.mode("overwrite").format("json").save(output_path)

print("Data processing and writing to S3 completed successfully.")

spark.stop()
"""

        with open('spark-job.py', 'w') as f:
            f.write(spark_script)

        s3.upload_file('spark-job.py', RAW_DATA_BUCKET, 'spark-job.py')
        logger.info("Uploaded Spark job script to S3.")

    except Exception as e:
        logger.error(f"Error creating/uploading Spark job script: {e}")

def setup_cloudwatch_alarms(cluster_id):
    try:
        cloudwatch = boto3.client(
            'cloudwatch',
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=REGION_NAME
        )

        # High CPU Utilization Alarm
        cloudwatch.put_metric_alarm(
            AlarmName='HighCPUUtilization',
            MetricName='CPUUtilization',
            Namespace='AWS/EMR',
            Statistic='Average',
            Period=300,
            EvaluationPeriods=1,
            Threshold=80.0,
            ComparisonOperator='GreaterThanOrEqualToThreshold',
            Dimensions=[
                {'Name': 'JobFlowId', 'Value': cluster_id}
            ],
            Unit='Percent'
        )
        logger.info("CloudWatch alarm set for high CPU utilization.")

        # Low Memory Utilization Alarm
        cloudwatch.put_metric_alarm(
            AlarmName='LowMemoryUtilization',
            MetricName='MemoryUtilization',
            Namespace='AWS/EMR',
            Statistic='Average',
            Period=300,
            EvaluationPeriods=1,
            Threshold=20.0,
            ComparisonOperator='LessThanOrEqualToThreshold',
            Dimensions=[
                {'Name': 'JobFlowId', 'Value': cluster_id}
            ],
            Unit='Percent'
        )
        logger.info("CloudWatch alarm set for low memory utilization.")

        # High Disk I/O Alarm
        cloudwatch.put_metric_alarm(
            AlarmName='HighDiskIOPS',
            MetricName='DiskWriteOps',
            Namespace='AWS/EMR',
            Statistic='Sum',
            Period=300,
            EvaluationPeriods=1,
            Threshold=1000,
            ComparisonOperator='GreaterThanOrEqualToThreshold',
            Dimensions=[
                {'Name': 'JobFlowId', 'Value': cluster_id}
            ],
            Unit='Count'
        )
        logger.info("CloudWatch alarm set for high disk I/O operations.")

    except cloudwatch.exceptions.ClientError as e:
        logger.error(f"CloudWatch ClientError: {e}")
    except Exception as e:
        logger.error(f"Error setting up CloudWatch alarms: {e}")

# Main function to run the pipeline
def main():
    # Step 1: Create IAM roles
    create_iam_roles()

    # Step 2: Create S3 buckets
    create_buckets()

    # Step 3: Simulate IoT sensor data and upload to S3
    simulate_iot_data()

    # Step 4: Create EMR cluster
    cluster_id = create_emr_cluster()

    if cluster_id:
        # Step 5: Create Spark job script and upload to S3
        create_spark_job_script()

        # Step 6: Submit Spark job to EMR
        submit_spark_job(cluster_id)

        # Step 7: Set up CloudWatch alarms
        setup_cloudwatch_alarms(cluster_id)

# Run the main function
if __name__ == "__main__":
    main()
