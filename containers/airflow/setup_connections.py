import json
import subprocess


def add_minio_connection() -> None:
    """
    Add a MinIO connection to Airflow using the CLI.

    This function sets up a connection to a MinIO instance by defining the
    necessary connection details and executing the corresponding Airflow CLI command.
    """
    # Define connection details
    conn_id = "aws_default"
    conn_type = "aws"
    access_key = "minio"
    secret_key = "minio123"
    region_name = "us-east-1"
    endpoint_url = "http://minio:9000"

    # Construct the extra JSON
    extra = {
        "aws_access_key_id": access_key,
        "aws_secret_access_key": secret_key,
        "region_name": region_name,
        "host": endpoint_url,
    }

    # Convert to JSON string
    extra_json = json.dumps(extra)

    # Define the CLI command
    command = [
        "airflow",
        "connections",
        "add",
        conn_id,
        "--conn-type",
        conn_type,
        "--conn-extra",
        extra_json,
    ]

    # Execute the command
    subprocess.run(command)


def add_dwh_postgres_connection() -> None:
    """
    Add a PostgreSQL data warehouse connection to Airflow using the CLI.

    This function sets up a connection to a PostgreSQL data warehouse by defining
    the necessary connection details and executing the corresponding Airflow CLI command.
    """
    # Define connection details
    connection_id = "dwh-conn"
    connection_type = "postgres"
    host = "postgres-dwh"
    port = "5432"
    login = "dwh"
    password = "dwh"
    schema = "video_games_dwh"

    # Define the CLI command
    cmd = [
        "airflow",
        "connections",
        "add",
        connection_id,
        "--conn-type",
        connection_type,
        "--conn-host",
        host,
        "--conn-port",
        port,
        "--conn-login",
        login,
        "--conn-password",
        password,
        "--conn-schema",
        schema,
    ]

    # Execute the command
    subprocess.run(cmd, capture_output=True, text=True)


# Add the connections
add_minio_connection()
add_dwh_postgres_connection()
