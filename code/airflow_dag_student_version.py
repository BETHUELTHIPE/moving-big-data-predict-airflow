import airflow
import json
import os
import csv
import boto3
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime
from pathlib import Path

HOME_DIR = "/opt/airflow"

# Insert your mount folder

# ==============================================================

# The default arguments for your Airflow, these have no reason to change for the purposes of this predict.
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}


# The function that uploads data to the RDS database, it is called upon later.
def upload_to_postgres(**kwargs):
    
    
    # Assuming you have the CSV file path stored in the "csv_file" variable
    csv_file = "C:\Users\Bethuel\Documents\MOVING BIG DATA\Data\Output"
    
    try:
        # Open the CSV file
        with open(csv_file, 'r') as file:
            # Read the CSV data
            csv_data = csv.reader(file)
            
            # Connect to your Postgres database
            # Assuming you have the database credentials stored in environment variables
            db_host = os.environ.get("localhost")
            db_user = os.environ.get("postgres")
            db_password = os.environ.get("23498812")
            db_name = os.environ.get("PostgreSQL 15")
            
            connection = psycopg2.connect(host=db_host, user=db_user, password=db_password, database=db_name)
            
            # Create a cursor object to execute SQL queries
            cursor = connection.cursor()
            
            # Iterate over the CSV data and insert into the database
            for row in csv_data:
                query = "INSERT INTO historical_stocks_data(stock_date, open_value, high_value,low_value,close_value,volume_traded,daily_percent_change,value_change,company_name) VALUES (%s, %s, %s,%s, %s, %s,%s, %s, %s)"
                cursor.execute(query, (row[0], row[1], row[2],row[3], row[4], row[5],row[6], row[7], row[8]))
            
            # Commit the changes to the database
            connection.commit()
            
            # Close the cursor and database connection
            cursor.close()
            connection.close()
            
            return "CSV uploaded to Postgres database"
        
    except Exception as e:
        # Handle the exception if any error occurs during the upload process
        return str(e)


def failure_sns(context):
    # Write a function that will send a failure SNS notification
    
    # Assuming you have configured your AWS credentials and region
    sns_client = boto3.client('sns', region_name='us-east-1')
    topic_arn = 'arn:aws:sns:eu-west-1:058761519052:de-mbd-predict-Bethuel-Moukangwe-SNS'
    
    message = f"Data upload failed for DAG: {context['task_instance'].dag_id}"
    
    try:
        response = sns_client.publish(
            TopicArn=topic_arn,
            Message=message
        )
        
        return f"Failure SNS notification sent: {response['MessageId']}"
    
    except Exception as e:
        # Handle the exception if any error occurs while sending the SNS notification
        return str(e)


def success_sns(context):
    # Write a function that will send a success SNS notification
    
    # Assuming you have configured your AWS credentials and region
    sns_client = boto3.client('sns', region_name='us-east-1')
    topic_arn = 'arn:aws:sns:eu-west-1:058761519052:de-mbd-predict-Bethuel-Moukangwe-SNS'
    
    message = f"Data upload succeeded for DAG: {context['task_instance'].dag_id}"
    
    try:
        response = sns_client.publish(
            TopicArn=topic_arn,
            Message=message
        )
        
        return f"Success SNS notification sent: {response['MessageId']}"
    
    except Exception as e:
        # Handle the exception if any error occurs while sending the SNS notification
        return str(e)
    
    # The dag configuration 
# Ensure your DAG calls on the success and failure functions above as it succeeds or fails.
dag = DAG(
    'data_upload',
    default_args=default_args,
    description='DAG for uploading data to Postgres database',
    schedule_interval=None,
    start_date=datetime(2023, 6, 19)
)


# Write your DAG tasks below 
upload_task = PythonOperator(
    task_id='upload_to_postgres',
    python_callable=upload_to_postgres,
    provide_context=True,
    dag=dag
)

failure_sns_task = PythonOperator(
    task_id='failure_sns',
    python_callable=failure_sns,
    provide_context=True,
    dag=dag
)

success_sns_task = PythonOperator(
    task_id='success_sns',
    python_callable=success_sns,
    provide_context=True,
    dag=dag
)


# Define your Task flow below
upload_task >> success_sns_task
upload_task >> failure_sns_task
