from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import requests
import psycopg2
import boto3
import csv
import os
from airflow.models import Variable

dt = datetime.today()  # Get timezone naive now
seconds = dt.timestamp()


#fcvgbhkjnl


# PostgreSQL connection details
PG_HOST = Variable.get("PG_HOST")
PG_DATABASE = Variable.get("PG_DATABASE")
PG_USER = Variable.get("PG_USER")
PG_PASSWORD = Variable.get("PG_PASSWORD")
PG_PORT = 5432

# S3 configuration
S3_BUCKET_NAME = "baisleylake"
S3_FILE_PATH = "airflow/" + str({seconds}) + "_coincap_data.csv"  # Path inside the S3 bucket
LOCAL_FILE_PATH = "coincap_data.csv"  # Temporary file path for CSV

# AWS Credentials (Ensure IAM role or set environment variables)
AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")



def get_var_regular_func():

    my_regular_var = Variable.get("my_regular_var", default_var=None)

    print(my_json_var)




def fetch_coincap_data(ti):
    """Fetch data from CoinCap API and push to XCom."""
    url = "https://api.coincap.io/v2/assets"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()["data"]
    
    print("fetch_crypto_data", data)
    
    # Push data to XCom
    ti.xcom_push(key="coincap_data", value=data)

def process_data(ti):
    """Extract relevant fields from CoinCap API response and push processed data to XCom."""
    coincap_data = ti.xcom_pull(task_ids="fetch_crypto_data", key="coincap_data")
    
    if not coincap_data:
        raise ValueError("No data received from CoinCap API")

    processed_data = [
        (item["id"], item["rank"], item["symbol"], item["name"], float(item["priceUsd"]), datetime.now())
        for item in coincap_data
    ]
    
    print("process_crypto_data", processed_data)

    # Push processed data to XCom
    ti.xcom_push(key="processed_data", value=processed_data)

def ingest_data_to_postgres(ti):
    """Ingest processed CoinCap data into PostgreSQL."""
    processed_data = ti.xcom_pull(task_ids="process_crypto_data", key="processed_data")

    if not processed_data:
        raise ValueError("No processed data available for ingestion")

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host=Variable.get("PG_HOST"),
        database=Variable.get("PG_DATABASE"),
        user=Variable.get("PG_USER"),
        password=Variable.get("PG_PASSWORD"),
        port=PG_PORT

    )
    cursor = conn.cursor()

    # Create table if it does not exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS coincap_data (
        id TEXT,
        rank TEXT,
        symbol TEXT,
        name TEXT,
        price_usd FLOAT,
        timestamp TIMESTAMP, 
        PRIMARY KEY(id, timestamp)
    );
    """
    cursor.execute(create_table_query)

    # Insert data
    insert_query = """
    INSERT INTO coincap_data (id, rank, symbol, name, price_usd, timestamp)
    VALUES (%s, %s, %s, %s, %s, %s);

    """
    
    cursor.executemany(insert_query, processed_data)
    
    # Commit and close connection
    conn.commit()
    cursor.close()
    conn.close()
    
    print("Successfully ingested data into PostgreSQL.")

def upload_to_s3(ti):
    """Save processed data to CSV and upload to S3."""
    processed_data = ti.xcom_pull(task_ids="process_crypto_data", key="processed_data")

    if not processed_data:
        raise ValueError("No processed data available for CSV export")

    # Write to CSV
    with open(LOCAL_FILE_PATH, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["id", "rank", "symbol", "name", "price_usd", "timestamp"])
        writer.writerows(processed_data)
    
    print(f"CSV saved locally at {LOCAL_FILE_PATH}")

    # Upload to S3
    s3_client = boto3.client(
        "s3",
        aws_access_key_id = Variable.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key = Variable.get("AWS_SECRET_ACCESS_KEY")
    )
    s3_client.upload_file(LOCAL_FILE_PATH, S3_BUCKET_NAME, S3_FILE_PATH)
    
    print(f"CSV successfully uploaded to s3://baisleylake/airflow")
    os.remove(LOCAL_FILE_PATH)
    print("removed staged file")
with DAG(
    "my_dag",
    start_date=datetime(2024, 2, 1),
    schedule_interval="@hourly",
    catchup=False
) as dag:

    fetch_data = PythonOperator(
        task_id="fetch_crypto_data",
        python_callable=fetch_coincap_data
    )

    process_crypto_data = PythonOperator(
        task_id="process_crypto_data",
        python_callable=process_data
    )

    ingest_data = PythonOperator(
        task_id="ingest_data_to_postgres",
        python_callable=ingest_data_to_postgres
    )

    upload_csv = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3
    )

    fetch_data >> process_crypto_data >> [ingest_data, upload_csv]
