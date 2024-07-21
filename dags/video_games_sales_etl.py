from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
with DAG(
    'test_dag_with_s3',
    default_args=default_args,
    description='A simple DAG to create an S3 bucket, save a word to a file, and insert into PostgreSQL',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    # Task 1: Create an S3 bucket
    create_s3_bucket = S3CreateBucketOperator(
        task_id='create_s3_bucket',
        bucket_name='my-airflow-bucket',
        aws_conn_id='aws_default',
        region_name='us-east-1'
    )
    

    # Task 2: Make a request to an API and save the word to a file in the S3 bucket
    def save_word_to_s3():
        response = requests.get('https://random-word-api.herokuapp.com/word')  # Replace with actual API
        word = response.json()[0]
        
        # Save word to a file
        filename = 'word.txt'
        with open(filename, 'w') as file:
            file.write(word)
        
        # Upload the file to S3
        s3_hook = S3Hook(aws_conn_id='aws_default')
        s3_hook.load_file(filename, key=filename, bucket_name='my-airflow-bucket', replace=True)
    
    save_word_to_s3_task = PythonOperator(
        task_id='save_word_to_s3',
        python_callable=save_word_to_s3
    )

    # Task 3: Create the words table in PostgreSQL if it doesn't exist
    create_words_table = PostgresOperator(
        task_id='create_words_table',
        postgres_conn_id='dwh-conn',
        sql="""
        CREATE TABLE IF NOT EXISTS words (
            id SERIAL PRIMARY KEY,
            word VARCHAR(255) NOT NULL
        );
        """
    )

    # Task 4: Load the file from S3 and insert the word into the words table
    def insert_word_to_postgres():
        s3_hook = S3Hook(aws_conn_id='aws_default')
        word_file = s3_hook.download_file(key='word.txt', bucket_name='my-airflow-bucket')
        
        with open(word_file, 'r') as file:
            word = file.read().strip()
        
        postgres_hook = PostgresHook(postgres_conn_id='dwh-conn')
        postgres_hook.run("INSERT INTO words (word) VALUES (%s)", parameters=(word,))
    
    insert_word_to_postgres_task = PythonOperator(
        task_id='insert_word_to_postgres',
        python_callable=insert_word_to_postgres
    )

    # Define task dependencies
    create_s3_bucket >> save_word_to_s3_task >> create_words_table >> insert_word_to_postgres_task
