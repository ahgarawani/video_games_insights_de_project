import logging
import os
import shutil

import kaggle
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from .utils import *


def extract_dataset() -> None:
    """
    Downloads the video games dataset using the Kaggle API. If the download fails,
    it falls back to using a local dataset.
    """
    try:
        # Use Kaggle API to download the dataset
        kaggle.api.dataset_download_files(
            'ujjwalaggarwal402/video-games-dataset', path='/tmp', unzip=True
        )
    except Exception as e:
        logging.error("Failed to download dataset from Kaggle: %s", e)
        # Fallback to local data if download fails
        shutil.copyfile('./data/raw/vg_data_dictionary.csv', '/tmp')
        shutil.copyfile('./data/raw/Video Games Data.csv', '/tmp')


def store_data_in_s3() -> None:
    """
    Uploads the video games dataset to an S3 bucket.
    """
    dataset_files = ['vg_data_dictionary.csv', 'Video Games Data.csv']
    # Initialize S3 hook
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = 'video-games-data-bucket'
    # Upload each dataset file to S3
    for dataset_file in dataset_files:
        s3_key = f'raw/{dataset_file}'
        s3_hook.load_file(
            filename=os.path.join('/tmp', dataset_file),
            key=s3_key,
            bucket_name=bucket_name,
            replace=True,
        )


def remodel_dataset() -> None:
    """
    Transforms the raw video games dataset into a star schema and uploads the
    transformed data to an S3 bucket.
    """
    # Initialize S3 hook
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = 'video-games-data-bucket'

    # Download the raw dataset from S3
    s3_hook.download_file(
        key='raw/Video Games Data.csv',
        bucket_name=bucket_name,
        preserve_file_name=True,
    )

    # Load the dataset into a DataFrame
    dataset_df = pd.read_csv('/tmp/Video Games Data.csv')

    # Drop rows with missing total_sales
    dataset_df.dropna(subset=['total_sales'], inplace=True)

    # Create dimension tables
    titles = create_dimension_table(dataset_df, 'title')
    consoles = create_dimension_table(dataset_df, 'console')
    genres = create_dimension_table(dataset_df, 'genre')
    publishers = create_dimension_table(dataset_df, 'publisher')
    developers = create_dimension_table(dataset_df, 'developer')

    # Merge the original DataFrame with dimension tables to replace text with IDs
    dataset_df = (
        dataset_df.merge(titles, on='title')
        .merge(consoles, on='console')
        .merge(genres, on='genre')
        .merge(publishers, on='publisher')
        .merge(developers, on='developer')
    )

    # Select and rename the columns for the fact table
    fact_columns = [
        'critic_score',
        'total_sales',
        'na_sales',
        'jp_sales',
        'pal_sales',
        'other_sales',
        'release_date',
        'last_update',
        'title_id',
        'console_id',
        'genre_id',
        'publisher_id',
        'developer_id',
    ]

    facts_table = dataset_df[fact_columns]
    facts_table['release_date'] = pd.to_datetime(
        facts_table['release_date'], format='%d-%m-%Y'
    ).dt.strftime('%Y-%m-%d')
    facts_table['last_update'] = pd.to_datetime(
        facts_table['last_update'], format='%d-%m-%Y'
    ).dt.strftime('%Y-%m-%d')

    # Save the tables to disk
    titles.to_csv('/tmp/titles_dim.csv', index=False)
    consoles.to_csv('/tmp/consoles_dim.csv', index=False)
    genres.to_csv('/tmp/genres_dim.csv', index=False)
    publishers.to_csv('/tmp/publishers_dim.csv', index=False)
    developers.to_csv('/tmp/developers_dim.csv', index=False)
    facts_table.to_csv('/tmp/facts.csv', index=False)

    # Upload the tables to S3
    s3_hook.load_file(
        filename='/tmp/titles_dim.csv',
        key='clean/titles_dim.csv',
        bucket_name=bucket_name,
        replace=True,
    )
    s3_hook.load_file(
        filename='/tmp/consoles_dim.csv',
        key='clean/consoles_dim.csv',
        bucket_name=bucket_name,
        replace=True,
    )
    s3_hook.load_file(
        filename='/tmp/genres_dim.csv',
        key='clean/genres_dim.csv',
        bucket_name=bucket_name,
        replace=True,
    )
    s3_hook.load_file(
        filename='/tmp/publishers_dim.csv',
        key='clean/publishers_dim.csv',
        bucket_name=bucket_name,
        replace=True,
    )
    s3_hook.load_file(
        filename='/tmp/developers_dim.csv',
        key='clean/developers_dim.csv',
        bucket_name=bucket_name,
        replace=True,
    )
    s3_hook.load_file(
        filename='/tmp/facts.csv',
        key='clean/facts.csv',
        bucket_name=bucket_name,
        replace=True,
    )


def load_csv_into_postgres_table(csv_name: str) -> None:
    """
    Loads a CSV file from an S3 bucket into a PostgreSQL table.

    Args:
        csv_name (str): The name of the CSV file (without extension).
    """
    # Initialize S3 hook
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = 'video-games-data-bucket'

    # Download the CSV file from S3
    s3_hook.download_file(
        key=f'clean/{csv_name}.csv',
        bucket_name=bucket_name,
        preserve_file_name=True,
    )

    # Load the CSV file into a DataFrame
    df = pd.read_csv(f'/tmp/{csv_name}.csv')

    # Construct a big insert query
    columns = ', '.join(df.columns)
    values = []
    for row in df.itertuples(index=False, name=None):
        values.append(f"({', '.join(map(escape_value, row))})")
    insert_query = f"""
        INSERT INTO {csv_name} ({columns})
        VALUES {', '.join(values)} 
        ON CONFLICT DO NOTHING
    """

    # Initialize PostgreSQL hook
    postgres_hook = PostgresHook(postgres_conn_id='dwh-conn')
    # Execute the insert query
    postgres_hook.run(insert_query)
