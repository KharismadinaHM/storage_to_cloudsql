import os
import time
import zipfile
from google.cloud import storage
from google.auth import default
from google.cloud.sql.connector import Connector
from googleapiclient.discovery import build
import pymysql
import subprocess

# Inisialisasi client Google Cloud Storage dan SQL Admin API
storage_client = storage.Client()
credentials, project = default()
sqladmin_service = build('sqladmin', 'v1', credentials=credentials)

# Nama bucket dan file
BUCKET_NAME = 'storage_to_cloud_sql'
DATABASE_NAME = 'testDB'
INSTANCE_CONNECTION_NAME = 'dev-kharismadina-hm:asia-southeast2:storage-to-sql-server-2019'
CLOUD_SQL_INSTANCE = 'storage-to-sql-server-2019'
CLOUD_SQL_USER = 'sqlserver'
CLOUD_SQL_PASSWORD = 'sqlserver'
TEMP_DIR = '/tmp'  # Direktori sementara untuk unzip file

# Fungsi untuk mengecek apakah ada file baru di Cloud Storage
def check_new_file(bucket_name):
    bucket = storage_client.get_bucket(bucket_name)
    blobs = list(bucket.list_blobs())

    # Cek apakah ada file
    if blobs:
        return blobs[0].name
    return None

# Fungsi untuk men-download dan men-unzip file ZIP
def download_and_unzip_file(bucket_name, file_name, destination_dir):
    # Download file dari Cloud Storage
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    zip_file_path = os.path.join(destination_dir, file_name)

    # Simpan file ZIP ke lokal
    blob.download_to_filename(zip_file_path)
    print(f"File {file_name} berhasil di-download ke {zip_file_path}")

    # Ekstraksi file ZIP
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(destination_dir)
        print(f"File {file_name} berhasil diekstrak ke {destination_dir}")

    # Mengembalikan daftar file yang diekstrak
    extracted_files = zip_ref.namelist()
    return extracted_files

# Fungsi untuk menghidupkan instance Cloud SQL
def start_cloud_sql(instance_name):
    print(f"Menyalakan Cloud SQL instance: {instance_name}")
    subprocess.run(f"gcloud sql instances patch {instance_name} --activation-policy=ALWAYS", shell=True)

# Fungsi untuk mematikan instance Cloud SQL
def stop_cloud_sql(instance_name):
    print(f"Mematikan Cloud SQL instance: {instance_name}")
    subprocess.run(f"gcloud sql instances patch {instance_name} --activation-policy=NEVER", shell=True)

# Fungsi untuk mengecek apakah Cloud SQL instance sudah ready
def wait_until_sql_ready(project, instance_name):
    while True:
        instance_status = sqladmin_service.instances().get(
            project=project,
            instance=instance_name
        ).execute()

        status = instance_status['state']
        print(f"Status Cloud SQL instance '{instance_name}': {status}")

        if status == 'RUNNABLE':
            print(f"Cloud SQL instance '{instance_name}' sudah siap!")
            break

        # Jika belum siap, tunggu 10 detik dan cek ulang
        time.sleep(10)

# Fungsi untuk mengirim file dari Cloud Storage ke Cloud SQL
def upload_to_cloud_sql(file_name):
    connector = Connector()

    # Koneksi ke database
    conn = connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pymysql",
        user=CLOUD_SQL_USER,
        password=CLOUD_SQL_PASSWORD,
        db=DATABASE_NAME
    )

    try:
        with conn.cursor() as cursor:
            # Membuka file dari lokal (setelah unzip)
            file_path = os.path.join(TEMP_DIR, file_name)
            with open(file_path, 'r') as f:
                data = f.read()

            # Memproses data, misalnya mengirim data ke tabel SQL
            for line in data.splitlines():
                # Misal file CSV dengan kolom dipisahkan koma
                values = line.split(',')
                cursor.execute(
                    "INSERT INTO nama_tabel (kolom1, kolom2) VALUES (%s, %s)", (values[0], values[1])
                )

            # Commit setelah semua data berhasil dimasukkan
            conn.commit()
            print(f"File {file_name} berhasil dimasukkan ke Cloud SQL")

    finally:
        conn.close()

# Skrip utama
def main():
    # Cek apakah ada file baru di Cloud Storage
    file_name = check_new_file(BUCKET_NAME)

    if file_name:
        print(f"File baru ditemukan: {file_name}")

        # Jika file adalah file zip, unzip terlebih dahulu
        if file_name.endswith('.zip'):
            extracted_files = download_and_unzip_file(BUCKET_NAME, file_name, TEMP_DIR)

            # Step 2: Menyalakan Cloud SQL
            start_cloud_sql(CLOUD_SQL_INSTANCE)

            # Tunggu hingga Cloud SQL siap
            wait_until_sql_ready(project, CLOUD_SQL_INSTANCE)

            # Step 3: Upload file yang diekstrak ke Cloud SQL
            for extracted_file in extracted_files:
                upload_to_cloud_sql(extracted_file)
        else:
            print("File bukan ZIP, langsung upload ke Cloud SQL")

            # Step 2: Menyalakan Cloud SQL
            start_cloud_sql(CLOUD_SQL_INSTANCE)

            # Tunggu hingga Cloud SQL siap
            wait_until_sql_ready(project, CLOUD_SQL_INSTANCE)

            # Step 3: Upload file langsung ke Cloud SQL
            upload_to_cloud_sql(file_name)

        # Step 4: Mematikan Cloud SQL
        stop_cloud_sql(CLOUD_SQL_INSTANCE)
    else:
        print("Tidak ada file baru di Cloud Storage.")

if __name__ == "__main__":
    main()
