import os
import time
import datetime
from minio import Minio
from minio.error import S3Error
from io import BytesIO


# Config
INPUT_FOLDER = "input"
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"
MINIO_BUCKET = "logs"

# Initialize MinIO client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# Create bucket if not exists
if not minio_client.bucket_exists(MINIO_BUCKET):
    minio_client.make_bucket(MINIO_BUCKET)
    print(f"Created bucket '{MINIO_BUCKET}'")
else:
    print(f"Bucket '{MINIO_BUCKET}' exists")

# Keep track of uploaded files with their last modified time
uploaded_files = {}

def upload_file_to_minio(file_path):
    try:
        with open(file_path, "rb") as f:
            data = f.read()
        
        data_stream = BytesIO(data)

        # Timestamp folder as YYYY-MM-DD
        date_path = datetime.datetime.now().strftime("%Y-%m-%d")
        filename = os.path.basename(file_path)
        object_path = f"{date_path}/{filename}"

        minio_client.put_object(
            MINIO_BUCKET,
            object_path,
            data_stream,
            length=len(data),
            content_type="text/plain"
        )
        print(f"Uploaded {filename} to MinIO at {object_path}")
    except Exception as e:
        print(f"Error uploading {file_path}: {e}")

def watch_folder():
    print(f"Watching folder '{INPUT_FOLDER}' for new or updated files...")
    while True:
        try:
            for filename in os.listdir(INPUT_FOLDER):
                file_path = os.path.join(INPUT_FOLDER, filename)
                if os.path.isfile(file_path):
                    last_modified = os.path.getmtime(file_path)
                    # Upload if file is new or updated
                    if (filename not in uploaded_files) or (uploaded_files[filename] < last_modified):
                        upload_file_to_minio(file_path)
                        uploaded_files[filename] = last_modified
            time.sleep(5)  # Check every 5 seconds
        except Exception as e:
            print(f"Error watching folder: {e}")
            time.sleep(5)

if __name__ == "__main__":
    # Create input folder if it doesn't exist
    if not os.path.exists(INPUT_FOLDER):
        os.makedirs(INPUT_FOLDER)
        print(f"Created input folder '{INPUT_FOLDER}'")

    watch_folder()
