import os
import gzip
import shutil
import logging
import subprocess
import time
import functions_framework 
import pymssql
from google.cloud import storage
from google.auth import default 
from googleapiclient.discovery import build 
from google.cloud.sql.connector import Connector 
from googleapiclient.errors import HttpError
from google.cloud.sql.connector import Connector


# from google.cloud import exceptions



# Inisialisasi logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# Inisialisasi client Google Cloud Storage dan SQL Admin API
storage_client = storage.Client()
credentials, project = default()
sqladmin_service = build('sqladmin', 'v1', credentials=credentials)

# Nama bucket dan file
BUCKET_NAME = 'aggibak'
DATABASE_NAME = 'testing_function'
INSTANCE_CONNECTION_NAME = 'poc-arthagraha:asia-southeast2:seacloud'
CLOUD_SQL_INSTANCE = 'seacloud'
CLOUD_SQL_USER = 'sqlserver'
CLOUD_SQL_PASSWORD = '1234'
TEMP_DIR = '/tmp'  # Direktori sementara untuk unzip file

# Fungsi untuk memastikan direktori ada
def ensure_directory_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
        logging.info(f"Directory {directory} created.")

def download_and_extract_gzip(bucket_name, file_name, destination_dir):
    # Memastikan direktori sementara ada
    ensure_directory_exists(destination_dir)

    # Memeriksa apakah file yang diberikan adalah file GZIP
    logging.info(f"TAHAP 1 : Download and extract gzip")
    if not file_name.endswith('.gz'):
        logging.warning(f"File {file_name} bukan file GZIP, tidak dapat diproses.")
        return []

    # Download file dari Cloud Storage
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    gzip_file_path = os.path.join(destination_dir, file_name)

    logging.info(f"Trying to download file from GCS: {file_name} to {gzip_file_path}")

    try:
        # Simpan file GZIP ke lokal
        blob.download_to_filename(gzip_file_path)
        logging.info(f"File {file_name} berhasil di-download ke {gzip_file_path}")

        # Ekstraksi file GZIP
        extracted_file_path = os.path.join(destination_dir, file_name.replace('.gz', ''))  # Menghilangkan .gz dari nama file

        with gzip.open(gzip_file_path, 'rb') as f_in:
            with open(extracted_file_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        logging.info(f"File {file_name} berhasil diekstrak ke {extracted_file_path}")

        return [extracted_file_path]

    except Exception as e:
        logging.error(f"Error downloading or extracting file: {e}")
        return []

# Fungsi untuk menghidupkan instance Cloud SQL
def start_cloud_sql(instance_name):
    logging.info(f"Menyalakan Cloud SQL instance: {instance_name}")
    subprocess.run(f"gcloud sql instances patch {instance_name} --activation-policy=ALWAYS", shell=True)

# Fungsi untuk mematikan instance Cloud SQL
def stop_cloud_sql(instance_name):
    logging.info(f"Mematikan Cloud SQL instance: {instance_name}")
    subprocess.run(f"gcloud sql instances patch {instance_name} --activation-policy=NEVER", shell=True)


# Fungsi untuk mengecek apakah Cloud SQL instance sudah siap
def wait_until_sql_ready(project, instance_name):
    while True:
        instance_status = sqladmin_service.instances().get(
            project=project,
            instance=instance_name
        ).execute()

        status = instance_status['state']
        logging.info(f"Status Cloud SQL instance '{instance_name}': {status}")

        if status == 'RUNNABLE':
            logging.info(f"Cloud SQL instance '{instance_name}' sudah siap!")
            break

        # Jika belum siap, tunggu 10 detik dan cek ulang
        time.sleep(10)

# Fungsi untuk mengirim file dari Cloud Storage ke Cloud SQL
def upload_to_cloud_sql(file_name):
    logging.info(f"TAHAP 4 : Upload File ke Cloud SQL")
    connector = Connector()

    # Koneksi ke database
    conn = connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pyodbc",
        user=CLOUD_SQL_USER,
        password=CLOUD_SQL_PASSWORD,
        db=DATABASE_NAME
    )

    try:
        with conn.cursor() as cursor:
            # File path ke .bak file
            file_path = os.path.join(TEMP_DIR, file_name)

            # Query untuk restore database
            restore_query = f"""
            RESTORE DATABASE [{DATABASE_NAME}]
            FROM DISK = N'{file_path}'
            WITH REPLACE
            """

            # Eksekusi query restore
            cursor.execute(restore_query)

            # Commit perubahan jika diperlukan
            conn.commit()

            logging.info(f"Database {DATABASE_NAME} berhasil direstore dari file {file_name}")
    except Exception as e:
        logging.error(f"Error saat melakukan restore database: {str(e)}")
        raise
    finally:
        conn.close()

# Fungsi utama untuk menangani event dari Cloud Storage menggunakan CloudEvent
@functions_framework.cloud_event
def hello_gcs(cloud_event):
    # Mengambil informasi file dari CloudEvent
    event_data = cloud_event.data
    file_name = event_data['name']
    bucket_name = event_data['bucket']

    logging.info(f"Event diterima. File baru ditemukan: {file_name} di bucket: {bucket_name}")

    # Jika file adalah file GZIP, ekstrak terlebih dahulu
    if file_name.endswith('.gz'):
        extracted_files = download_and_extract_gzip(bucket_name, file_name, TEMP_DIR)

        # Step 2: Menyalakan Cloud SQL
        start_cloud_sql(CLOUD_SQL_INSTANCE)

        # Tunggu hingga Cloud SQL siap
        wait_until_sql_ready(project, CLOUD_SQL_INSTANCE)

        # Step 3: Upload file yang diekstrak ke Cloud SQL
        for extracted_file in extracted_files:
            upload_to_cloud_sql(extracted_file)
    else:
        logging.info(f"File {file_name} bukan GZIP, langsung upload ke Cloud SQL")

        # Step 2: Menyalakan Cloud SQL
        start_cloud_sql(CLOUD_SQL_INSTANCE)

        # Tunggu hingga Cloud SQL siap
        wait_until_sql_ready(project, CLOUD_SQL_INSTANCE)

        # Step 3: Upload file langsung ke Cloud SQL
        upload_to_cloud_sql(file_name)

    # Step 4: Mematikan Cloud SQL
    stop_cloud_sql(CLOUD_SQL_INSTANCE)
