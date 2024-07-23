from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from dag_tasks import *

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
    'video_games_sales_etl_dag',
    default_args=default_args,
    description='An end-to-end pipeline to extract, transform, load and visualize a dataset of video game sales',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    # Stage 1: Create an S3 bucket
    create_s3_bucket = S3CreateBucketOperator(
        task_id='create_s3_bucket',
        bucket_name='video-games-data-bucket',
        aws_conn_id='aws_default',
        region_name='us-east-1',
    )

    # Stage 2: Download the dataset and store it in the S3 bucket
    extract_dataset_task = PythonOperator(
        task_id='extract_dataset', python_callable=extract_dataset
    )

    # Stage 3: Store the dataset in the S3 bucket
    store_data_in_s3_task = PythonOperator(
        task_id='store_data_in_s3', python_callable=store_data_in_s3
    )

    # Stage 4: Transform or remodel the data to follow the dimensional model
    remodel_dataset_task = PythonOperator(
        task_id='remodel_dataset', python_callable=remodel_dataset
    )

    # Stage 5: Create corresponding tables in the data warehouse
    create_titles_dim_table_task = PostgresOperator(
        task_id='create_titles_dim_table',
        postgres_conn_id='dwh-conn',
        sql=create_dim_table_query_constructor('title'),
    )
    create_consoles_dim_table_task = PostgresOperator(
        task_id='create_consoles_dim_table',
        postgres_conn_id='dwh-conn',
        sql=create_dim_table_query_constructor('console'),
    )
    create_genres_dim_table_task = PostgresOperator(
        task_id='create_genres_dim_table',
        postgres_conn_id='dwh-conn',
        sql=create_dim_table_query_constructor('genre'),
    )
    create_publishers_dim_table_task = PostgresOperator(
        task_id='create_publishers_dim_table',
        postgres_conn_id='dwh-conn',
        sql=create_dim_table_query_constructor('publisher'),
    )
    create_developers_dim_table_task = PostgresOperator(
        task_id='create_developers_dim_table',
        postgres_conn_id='dwh-conn',
        sql=create_dim_table_query_constructor('developer'),
    )

    # Stage 6: Load the remodeled dimensions into their corresponding tables in the data warehouse
    populate_titles_dim_table_task = PythonOperator(
        task_id='populate_titles_dim_table',
        python_callable=load_csv_into_postgres_table,
        op_kwargs={'csv_name': 'titles_dim'},
    )
    populate_consoles_dim_table_task = PythonOperator(
        task_id='populate_consoles_dim_table',
        python_callable=load_csv_into_postgres_table,
        op_kwargs={'csv_name': 'consoles_dim'},
    )
    populate_genres_dim_table_task = PythonOperator(
        task_id='populate_genres_dim_table',
        python_callable=load_csv_into_postgres_table,
        op_kwargs={'csv_name': 'genres_dim'},
    )
    populate_publishers_dim_table_task = PythonOperator(
        task_id='populate_publishers_dim_table',
        python_callable=load_csv_into_postgres_table,
        op_kwargs={'csv_name': 'publishers_dim'},
    )
    populate_developers_dim_table_task = PythonOperator(
        task_id='populate_developers_dim_table',
        python_callable=load_csv_into_postgres_table,
        op_kwargs={'csv_name': 'developers_dim'},
    )

    # Stage 7: Create a table for the facts in the data warehouse
    create_facts_table_task = PostgresOperator(
        task_id='create_facts_table',
        postgres_conn_id='dwh-conn',
        sql="""
        CREATE TABLE IF NOT EXISTS facts (
            id SERIAL PRIMARY KEY,
            title_id INT,
            console_id INT,
            genre_id INT,
            publisher_id INT,
            developer_id INT,
            release_date DATE,
            last_update DATE,
            critic_score FLOAT,
            total_sales FLOAT NOT NULL,
            na_sales FLOAT,
            jp_sales FLOAT,
            pal_sales FLOAT,
            other_sales FLOAT,
            FOREIGN KEY (title_id) REFERENCES titles_dim(title_id),
            FOREIGN KEY (console_id) REFERENCES consoles_dim(console_id),
            FOREIGN KEY (genre_id) REFERENCES genres_dim(genre_id),
            FOREIGN KEY (publisher_id) REFERENCES publishers_dim(publisher_id),
            FOREIGN KEY (developer_id) REFERENCES developers_dim(developer_id)
        );
        """,
    )

    # Stage 8: Load the facts into the corresponding table in the data warehouse
    populate_facts_table_task = PythonOperator(
        task_id='populate_facts_table',
        python_callable=load_csv_into_postgres_table,
        op_kwargs={'csv_name': 'facts'},
    )

    # Define task dependencies
    (
        create_s3_bucket
        >> extract_dataset_task
        >> store_data_in_s3_task
        >> remodel_dataset_task
    )
    (
        remodel_dataset_task
        >> create_titles_dim_table_task
        >> populate_titles_dim_table_task
        >> create_facts_table_task
    )
    (
        remodel_dataset_task
        >> create_consoles_dim_table_task
        >> populate_consoles_dim_table_task
        >> create_facts_table_task
    )
    (
        remodel_dataset_task
        >> create_genres_dim_table_task
        >> populate_genres_dim_table_task
        >> create_facts_table_task
    )
    (
        remodel_dataset_task
        >> create_publishers_dim_table_task
        >> populate_publishers_dim_table_task
        >> create_facts_table_task
    )
    (
        remodel_dataset_task
        >> create_developers_dim_table_task
        >> populate_developers_dim_table_task
        >> create_facts_table_task
    )
    create_facts_table_task >> populate_facts_table_task
