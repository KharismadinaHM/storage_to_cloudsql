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

# Inisialisasi logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# Inisialisasi client Google Cloud Storage dan SQL Admin API
storage_client = storage.Client()
credentials, project = default()
sqladmin_service = build('sqladmin', 'v1', credentials=credentials)

# Nama bucket dan file
BUCKET_NAME = 'aggibak'
INSTANCE_CONNECTION_NAME = 'poc-arthagraha:asia-southeast2:seacloud'
CLOUD_SQL_INSTANCE = 'seacloud'
DATABASE_NAME = 'BISA'
BAK_FILE = 'AdventureWorksLT2022.bak'

# Fungsi untuk mengeksekusi perintah gcloud
def execute_gcloud_command(command):
    try:
        logging.info(f"Menjalankan perintah: {' '.join(command)}")
        result = subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logging.info(f"Output: {result.stdout.decode('utf-8')}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error saat menjalankan perintah gcloud: {e.stderr.decode('utf-8')}")
        raise

# Fungsi untuk menghidupkan instance Cloud SQL menggunakan API
def start_cloud_sql(project, instance_name):
    logging.info(f"TAHAP 2 : Start Cloud SQL")
    try:
        request = sqladmin_service.instances().patch(
            project=project,
            instance=instance_name,
            body={"settings": {"activationPolicy": "ALWAYS"}}
        )
        response = request.execute()
        logging.info(f"Cloud SQL instance '{instance_name}' sedang dinyalakan.")
        return response
    except Exception as e:
        logging.error(f"Error starting Cloud SQL instance: {e}")
        return None

# Fungsi untuk mematikan instance Cloud SQL menggunakan API
def stop_cloud_sql(project, instance_name):
    logging.info(f"TAHAP 4 : Matikan Cloud SQL")
    try:
        request = sqladmin_service.instances().patch(
            project=project,
            instance=instance_name,
            body={"settings": {"activationPolicy": "NEVER"}}
        )
        response = request.execute()
        logging.info(f"Cloud SQL instance '{instance_name}' sedang dimatikan.")
        return response
    except Exception as e:
        logging.error(f"Error stopping Cloud SQL instance: {e}")
        return None

# Fungsi untuk mengecek apakah Cloud SQL instance sudah siap
def wait_until_sql_ready(project, instance_name):
    logging.info(f"TAHAP 3 : Tunggu Cloud SQL Siap")
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

# Fungsi untuk melakukan restore database menggunakan gcloud
def restore_database_with_gcloud(bak_file_path):
    # Step 1: Jalankan perintah gcloud sql import bak
    import_bak_command = [
        'gcloud', 'sql', 'import', 'bak', CLOUD_SQL_INSTANCE, bak_file_path,
        '--database=' + DATABASE_NAME, '--bak-type=FULL', '--no-recovery'
    ]
    execute_gcloud_command(import_bak_command)

    # Step 2: Jalankan perintah recovery-only
    recovery_command = [
        'gcloud', 'sql', 'import', 'bak', CLOUD_SQL_INSTANCE,
        '--database=' + DATABASE_NAME, '--recovery-only'
    ]
    execute_gcloud_command(recovery_command)

# Fungsi utama untuk menangani event dari Cloud Storage menggunakan CloudEvent
@functions_framework.cloud_event
def hello_gcs(cloud_event):
    # Mengambil informasi file dari CloudEvent
    event_data = cloud_event.data
    file_name = event_data.get('name')
    bucket_name = event_data.get('bucket')

    logging.info(f"Event diterima. File baru ditemukan: {file_name} di bucket: {bucket_name}")

    # Step 1: Pastikan file berada di Google Cloud Storage dan merupakan file .bak
    if file_name.endswith('.bak'):
        bak_file_path = f"gs://{bucket_name}/{file_name}"

        # Step 2: Menyalakan Cloud SQL
        start_cloud_sql(project, CLOUD_SQL_INSTANCE)

        # Tunggu hingga Cloud SQL siap
        wait_until_sql_ready(project, CLOUD_SQL_INSTANCE)

        # Step 3: Lakukan restore menggunakan perintah gcloud sql import bak
        restore_database_with_gcloud(bak_file_path)
    else:
        logging.error(f"File {file_name} bukan file .bak, tidak dapat diproses.")

    # Step 4: Mematikan Cloud SQL
    stop_cloud_sql(project, CLOUD_SQL_INSTANCE)
