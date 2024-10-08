import os
import gzip
import shutil
import logging
import subprocess
import time
import functions_framework 
from google.cloud import storage
from google.auth import default 
from googleapiclient.discovery import build 
from googleapiclient.errors import HttpError
from google.cloud.sql.connector import Connector, IPTypes
import sqlalchemy
import pytds

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
    subprocess.run(f"gcloud sql instances patch {instance_name} --activation-policy=ALWAYS", shell=True, check=True)

# Fungsi untuk mematikan instance Cloud SQL
def stop_cloud_sql(instance_name):
    logging.info(f"Mematikan Cloud SQL instance: {instance_name}")
    subprocess.run(f"gcloud sql instances patch {instance_name} --activation-policy=NEVER", shell=True, check=True)

# Fungsi untuk mengecek apakah Cloud SQL instance sudah siap
def wait_until_sql_ready(project, instance_name):
    while True:
        try:
            instance_status = sqladmin_service.instances().get(
                project=project,
                instance=instance_name
            ).execute()

            status = instance_status.get('state', 'UNKNOWN')
            logging.info(f"Status Cloud SQL instance '{instance_name}': {status}")

            if status == 'RUNNABLE':
                logging.info(f"Cloud SQL instance '{instance_name}' sudah siap!")
                break

            # Jika belum siap, tunggu 10 detik dan cek ulang
            time.sleep(10)
        except HttpError as e:
            logging.error(f"Error checking instance status: {e}")
            time.sleep(10)

# Fungsi untuk membuat koneksi menggunakan SQLAlchemy dan pytds
def connect_with_connector() -> sqlalchemy.engine.base.Engine:
    def getconn() -> pytds.Connection:
        connector = Connector()
        conn = connector.connect(
            INSTANCE_CONNECTION_NAME,  # Cloud SQL connection name
            "pytds",
            user=CLOUD_SQL_USER,
            password=CLOUD_SQL_PASSWORD,
            db=DATABASE_NAME,
            ip_type=IPTypes.PRIVATE
        )
        return conn

    engine = sqlalchemy.create_engine(
        "mssql+pytds://",
        creator=getconn,
    )
    logging.info(f"BERHASIL CONNECT SQL SERVER!")
    return engine

# Fungsi untuk mengirim file dari Cloud Storage ke Cloud SQL
def upload_to_cloud_sql(file_path):
    logging.info(f"TAHAP 4 : Upload File ke Cloud SQL")
    engine = connect_with_connector()

    try:
        with engine.connect() as connection:
            # Query untuk restore database
            restore_query = f"""
            RESTORE DATABASE [{DATABASE_NAME}]
            FROM DISK = N'{file_path}'
            WITH REPLACE
            """

            # Eksekusi query restore
            connection.execute(sqlalchemy.text(restore_query))
            logging.info(f"Database {DATABASE_NAME} berhasil direstore dari file {file_path}")
    except Exception as e:
        logging.error(f"Error saat melakukan restore database: {str(e)}")
        raise
    finally:
        engine.dispose()

# Fungsi utama untuk menangani event dari Cloud Storage menggunakan CloudEvent
@functions_framework.cloud_event
def hello_gcs(cloud_event):
    # Mengambil informasi file dari CloudEvent
    event_data = cloud_event.data
    file_name = event_data.get('name')
    bucket_name = event_data.get('bucket')

    logging.info(f"Event diterima. File baru ditemukan: {file_name} di bucket: {bucket_name}")

    if not file_name or not bucket_name:
        logging.error("Event data tidak lengkap. 'name' atau 'bucket' tidak ditemukan.")
        return

    # Jika file adalah file GZIP, ekstrak terlebih dahulu
    if file_name.endswith('.gz'):
        extracted_files = download_and_extract_gzip(bucket_name, file_name, TEMP_DIR)

        if extracted_files:
            try:
                # Step 2: Menyalakan Cloud SQL
                start_cloud_sql(CLOUD_SQL_INSTANCE)

                # Tunggu hingga Cloud SQL siap
                wait_until_sql_ready(project, CLOUD_SQL_INSTANCE)

                # Step 3: Upload file yang diekstrak ke Cloud SQL
                for extracted_file in extracted_files:
                    upload_to_cloud_sql(extracted_file)
            except Exception as e:
                logging.error(f"Proses upload gagal: {e}")
            finally:
                # Step 4: Mematikan Cloud SQL
                stop_cloud_sql(CLOUD_SQL_INSTANCE)
    else:
        logging.info(f"File {file_name} bukan GZIP, langsung upload ke Cloud SQL")

        try:
            # Step 2: Menyalakan Cloud SQL
            start_cloud_sql(CLOUD_SQL_INSTANCE)

            # Tunggu hingga Cloud SQL siap
            wait_until_sql_ready(project, CLOUD_SQL_INSTANCE)

            # Step 3: Upload file langsung ke Cloud SQL
            file_path = os.path.join(TEMP_DIR, file_name)
            download_and_extract_gzip(bucket_name, file_name, TEMP_DIR)  # Pastikan file tersedia di TEMP_DIR
            upload_to_cloud_sql(file_path)
        except Exception as e:
            logging.error(f"Proses upload gagal: {e}")
        finally:
            # Step 4: Mematikan Cloud SQL
            stop_cloud_sql(CLOUD_SQL_INSTANCE)
