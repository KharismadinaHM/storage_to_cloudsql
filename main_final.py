import os, sys, logging, subprocess, time, functions_framework # type: ignore
from google.auth.transport.requests import Request
from google.cloud import storage
from google.auth import default 
from googleapiclient.discovery import build 
from googleapiclient.errors import HttpError
from google.cloud.sql.connector import Connector, IPTypes
import sqlalchemy
import requests
import pytds

# Setting logging
logging.basicConfig(level=logging.warning, format='%(asctime)s %(levelname)s: %(message)s')

# Set class
storage_client = storage.Client()
credentials, project = default()
sqladmin_service = build('sqladmin', 'v1', credentials=credentials)

# Define constant variable
BUCKET_NAME = 'agi2_automatic_restore_bucket'
# INSTANCE_CONNECTION_NAME = 'poc-arthagraha:asia-southeast2:seacloud-clone'
CLOUD_SQL_INSTANCE = 'seacloud-clone'
CLOUD_SQL_USER = 'sqlserver'
CLOUD_SQL_PASSWORD = '1234'
TEMP_DIR = '/tmp'  # Direktori sementara untuk unzip file

def check_file_name(file_name, destination_dir):

    # Memeriksa apakah file yang diberikan adalah file GZIP atau BAK
    logging.warning("TAHAP 1 : Download and extract gzip")
    try:
        if file_name.endswith('.gz'):
            logging.warning(f"=========== Terdeteksi file {file_name} berformat .gz, segera diproses")
            file_parts = file_name.split('.')[0].split('_')  # Pisah dengan '_' dan hilangkan ekstensi
            file_name = os.path.join(destination_dir, file_name.replace('.gz', ''))  # Menghilangkan .gz dari nama file
            region = file_parts[0].lower()  # 'sea'
            environment = file_parts[1].lower()  # 'uat' 
            date = file_parts[2]  # '20240807'
            
            # Membentuk nama database dinamis
            database_name = f"sea_agi_db"
            return database_name
        
        elif file_name.endswith('.bak'):
            logging.warning(f"=========== Terdeteksi file {file_name} berformat .bak, segera diproses")
            file_parts = file_name.split('.')[0].split('_')  # Pisah dengan '_' dan hilangkan ekstensi
            file_name = os.path.join(destination_dir, file_name.replace('.bak', ''))  # Menghilangkan .gz dari nama file
            region = file_parts[0].lower()  # 'sea'
            environment = file_parts[1].lower()  # 'uat'
            date = file_parts[2]  # '20240807'

            # Membentuk nama database dinamis
            database_name = f"sea_agi_db"
            return database_name
        else:
            logging.error(f'=========== Format dari file {file_name} tidak didukung!')
            sys.exit(1)  # Menghentikan program jika format file tidak didukung
    except Exception as e:
        logging.error(f'=========== Terjadi kesalahan dalam pengecekan format file: {e}')
        sys.exit(1)

# Fungsi untuk memeriksa status instance Cloud SQL
def get_instance_status(instance_name, project):
    request = sqladmin_service.instances().get(project=project, instance=instance_name)
    response = request.execute()
    return response['state']

# Fungsi untuk menyalakan Cloud SQL instance jika belum aktif
def start_cloud_sql(instance_name, project):
    logging.warning(f"TAHAP 2 : Start Cloud SQL")
    try:
        # Menggunakan API Cloud SQL untuk mengubah kebijakan aktivasi
        request = sqladmin_service.instances().patch(
            project=project,
            instance=instance_name,
            body={"settings": {"activationPolicy": "ALWAYS"}}
        )
        response = request.execute()
        logging.warning(f"Cloud SQL instance '{instance_name}' sedang dinyalakan.")
        return response
    except Exception as e:
        logging.error(f"Error starting Cloud SQL instance: {e}")
        return None

def stop_cloud_sql(instance_name, project):
    logging.warning(f"=========== Mematikan instance {instance_name}...")

    # Patch untuk mengubah activation policy menjadi 'NEVER'
    request = sqladmin_service.instances().patch(
        project=project,
        instance=instance_name,
        body={"settings": {"activationPolicy": "NEVER"}}
    )
    
    response = request.execute()
    logging.warning(f"Response: {response}")

# Fungsi untuk mengecek apakah Cloud SQL instance sudah siap
def wait_until_sql_ready(project, instance_name):
    while True:
        try:
            instance_status = sqladmin_service.instances().get(
                project=project,
                instance=instance_name
            ).execute()

            status = instance_status.get('state', 'UNKNOWN')
            logging.warning(f"=========== Status Cloud SQL instance '{instance_name}': {status}")

            if status == 'RUNNABLE':
                logging.warning(f"=========== Cloud SQL instance '{instance_name}' sudah siap!")
                break

            # Jika belum siap, tunggu 10 detik dan cek ulang
            time.sleep(10)
        except HttpError as e:
            logging.error(f"=========== Error checking instance status: {e}")
            time.sleep(10)

# def check_and_delete_existing_db(file_name, instance_name, project):
#     logging.warning(f"=========== Memeriksa apakah database {file_name} sudah ada...")

#     # Mendapatkan daftar database dari Cloud SQL
#     request = sqladmin_service.databases().list(project=project, instance=instance_name)
#     response = request.execute()

#     # Memproses response untuk mendapatkan list nama database
#     database_list = [db['name'] for db in response.get('items', [])]

#     # Mengecek apakah file_name ada dalam daftar database
#     if file_name in database_list:
#         logging.warning(f"=========== Database {file_name} ditemukan, akan dihapus untuk restore.")
#         # Menghapus database jika ditemukan
#         delete_request = sqladmin_service.databases().delete(project=project, instance=instance_name, database=file_name)
#         delete_response = delete_request.execute()
#         logging.warning(f"=========== Database {file_name} berhasil dihapus. Response: {delete_response}")
#     else:
#         logging.warning(f"=========== Database {file_name} tidak ditemukan, melanjutkan tanpa menghapus.")

def check_and_delete_existing_db(file_name, instance_name, project):
    file_name = 'sea_agi_db'
    logging.warning(f"=========== Memeriksa apakah database {file_name} sudah ada...")

    # Mendapatkan daftar database dari Cloud SQL
    request = sqladmin_service.databases().list(project=project, instance=instance_name)
    response = request.execute()

    # Memproses response untuk mendapatkan list nama database
    database_list = [db['name'] for db in response.get('items', [])]

    # Mengecek apakah file_name ada dalam daftar database
    if file_name in database_list:
        logging.warning(f"=========== Database {file_name} ditemukan, akan dihapus untuk restore.")
        # Menghapus database jika ditemukan
        delete_request = sqladmin_service.databases().delete(project=project, instance=instance_name, database=file_name)
        delete_response = delete_request.execute()
        logging.warning(f"=========== Database {file_name} berhasil dihapus. Response: {delete_response}")
    else:
        logging.warning(f"=========== Database {file_name} tidak ditemukan, melanjutkan tanpa menghapus.")

# Fungsi untuk mengirim file dari Cloud Storage ke Cloud SQL
def restore_backup(bucket_name, file_name, instance_name, project):
    database_name = check_file_name(file_name, TEMP_DIR)
    logging.warning(f"=========== TAHAP 4 : Restore file to Cloud SQL")

    if file_name.endswith('.bak'):
        # Menggunakan Cloud SQL Admin API untuk restore .bak file
        body = {
            'importContext': {
                'fileType': 'BAK',
                'uri': f'gs://{bucket_name}/{file_name}',
                'database': f'{database_name}'
            }
        }
        request = sqladmin_service.instances().import_(project=project, instance=instance_name, body=body)
        response = request.execute()
        logging.warning(f"=========== Restore .bak file sedang diproses. Response: {response}")

    elif file_name.endswith('.gz'):
        # Menggunakan Cloud SQL Admin API untuk restore .gz file
        body = {
            'importContext': {
                'fileType': 'SQL',
                'uri': f'gs://{bucket_name}/{file_name}',
                'database': f'{database_name}'
            }
        }
        request = sqladmin_service.instances().import_(project=project, instance=instance_name, body=body)
        response = request.execute()
        logging.warning(f"=========== Restore .gz file sedang diproses. Response: {response}")

# Fungsi utama untuk menangani event dari Cloud Storage menggunakan CloudEvent
@functions_framework.cloud_event
def hello_gcs(cloud_event):
    try:
        # Mengambil informasi file dari CloudEvent
        event_data = cloud_event.data
        file_name = event_data.get('name')
        bucket_name = event_data.get('bucket')

        logging.warning(f"=========== Event diterima. File baru ditemukan: {file_name} di bucket: {bucket_name}")

        check_file_name(file_name, TEMP_DIR)

        # start_cloud_sql(CLOUD_SQL_INSTANCE, project)

        # wait_until_sql_ready(project, CLOUD_SQL_INSTANCE)

        check_and_delete_existing_db(file_name, CLOUD_SQL_INSTANCE, project)

        restore_backup(bucket_name, file_name, CLOUD_SQL_INSTANCE, project)

        # stop_cloud_sql(CLOUD_SQL_INSTANCE, project)

    except Exception as e:
        logging.error(f"=========== Terjadi kesalahan di main function: {str(e)}")
        # Optional: cleanup atau kirim notifikasi jika error terjadi
