import os
import time
import zipfile
from google.cloud import storage
from google.auth import default
from google.cloud.sql.connector import Connector
from googleapiclient.discovery import build
import pymysql
import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

# Inisialisasi client Google Cloud Storage dan SQL Admin API
storage_client = storage.Client()
credentials, project = default()
sqladmin_service = build('sqladmin', 'v1', credentials=credentials)

# Konfigurasi Airflow
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 1),
    'retries': 1,
}

BUCKET_NAME = 'storage_to_cloud_sql'
DATABASE_NAME = 'testDB'
INSTANCE_CONNECTION_NAME = 'dev-kharismadina-hm:asia-southeast2:storage-to-sql-server-2019'
CLOUD_SQL_INSTANCE = 'storage-to-sql-server-2019'
CLOUD_SQL_USER = 'sqlserver'
CLOUD_SQL_PASSWORD = 'sqlserver'
TEMP_DIR = '/tmp'

# Fungsi untuk mengecek file baru di Cloud Storage
def check_new_file(**kwargs):
    bucket = storage_client.get_bucket(BUCKET_NAME)
    blobs = list(bucket.list_blobs())

    # Cek apakah ada file
    if blobs:
        file_name = blobs[0].name
        kwargs['ti'].xcom_push(key='file_name', value=file_name)
        return file_name
    return None

# Fungsi untuk mendownload dan unzip file ZIP
def download_and_unzip_file(**kwargs):
    file_name = kwargs['ti'].xcom_pull(key='file_name')
    if file_name.endswith('.zip'):
        bucket = storage_client.get_bucket(BUCKET_NAME)
        blob = bucket.blob(file_name)
        zip_file_path = os.path.join(TEMP_DIR, file_name)
        blob.download_to_filename(zip_file_path)

        # Unzip the file
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(TEMP_DIR)
            extracted_files = zip_ref.namelist()
            kwargs['ti'].xcom_push(key='extracted_files', value=extracted_files)
    else:
        print("File bukan ZIP")

# Fungsi untuk upload ke Cloud SQL
def upload_to_cloud_sql(**kwargs):
    extracted_files = kwargs['ti'].xcom_pull(key='extracted_files')
    if not extracted_files:  # If no extracted files, use original file
        extracted_files = [kwargs['ti'].xcom_pull(key='file_name')]

    connector = Connector()
    conn = connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pymysql",
        user=CLOUD_SQL_USER,
        password=CLOUD_SQL_PASSWORD,
        db=DATABASE_NAME
    )

    try:
        with conn.cursor() as cursor:
            for file_name in extracted_files:
                file_path = os.path.join(TEMP_DIR, file_name)
                with open(file_path, 'r') as f:
                    data = f.read()

                # Assume CSV, split by comma
                for line in data.splitlines():
                    values = line.split(',')
                    cursor.execute(
                        "INSERT INTO nama_tabel (kolom1, kolom2) VALUES (%s, %s)", (values[0], values[1])
                    )
            conn.commit()
            print(f"File {file_name} berhasil dimasukkan ke Cloud SQL")
    finally:
        conn.close()

# Fungsi untuk menghidupkan Cloud SQL
def start_cloud_sql():
    subprocess.run(f"gcloud sql instances patch {CLOUD_SQL_INSTANCE} --activation-policy=ALWAYS", shell=True)

# Fungsi untuk mematikan Cloud SQL
def stop_cloud_sql():
    subprocess.run(f"gcloud sql instances patch {CLOUD_SQL_INSTANCE} --activation-policy=NEVER", shell=True)

# Fungsi untuk mengecek apakah Cloud SQL siap
def wait_until_sql_ready():
    while True:
        instance_status = sqladmin_service.instances().get(
            project=project,
            instance=CLOUD_SQL_INSTANCE
        ).execute()

        status = instance_status['state']
        print(f"Status Cloud SQL instance '{CLOUD_SQL_INSTANCE}': {status}")

        if status == 'RUNNABLE':
            break
        time.sleep(10)

# Membuat DAG Airflow
with DAG('gcs_to_cloud_sql_dag', default_args=default_args, schedule_interval='@daily') as dag:

    # Task 1: Cek file baru di GCS
    check_file_task = PythonOperator(
        task_id='check_new_file',
        python_callable=check_new_file,
        provide_context=True
    )

    # Task 2: Download dan unzip file
    download_file_task = PythonOperator(
        task_id='download_and_unzip_file',
        python_callable=download_and_unzip_file,
        provide_context=True
    )

    # Task 3: Start Cloud SQL instance
    start_sql_task = BashOperator(
        task_id='start_cloud_sql',
        bash_command=f"gcloud sql instances patch {CLOUD_SQL_INSTANCE} --activation-policy=ALWAYS"
    )

    # Task 4: Tunggu sampai Cloud SQL siap
    wait_sql_ready_task = PythonOperator(
        task_id='wait_until_sql_ready',
        python_callable=wait_until_sql_ready
    )

    # Task 5: Upload file ke Cloud SQL
    upload_to_sql_task = PythonOperator(
        task_id='upload_to_cloud_sql',
        python_callable=upload_to_cloud_sql,
        provide_context=True
    )

    # Task 6: Stop Cloud SQL instance
    stop_sql_task = BashOperator(
        task_id='stop_cloud_sql',
        bash_command=f"gcloud sql instances patch {CLOUD_SQL_INSTANCE} --activation-policy=NEVER"
    )

    # Menentukan urutan eksekusi task
    check_file_task >> download_file_task >> start_sql_task >> wait_sql_ready_task >> upload_to_sql_task >> stop_sql_task
