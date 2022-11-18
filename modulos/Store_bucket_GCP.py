import os
from google.cloud import storage

from cfg import bucket_name, bucket_folder, project_id
from variables import datasets

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"
] = r"../credenciales/reba-challenge-bucket-a7afe14ad9e8.json"

# client = storage.Client.from_service_account_json(
#     json_credentials_path=r"./credenciales/reba-challenge-bucket-a7afe14ad9e8.json"
# )


def upload_blob(project_id, bucket_name, bucket_folder, datasets):
    """Uploads a file to the bucket."""

    storage_client = storage.Client(project_id)
    try:
        bucket = storage_client.get_bucket(bucket_name)
    except:
        bucket = storage_client.bucket(bucket_name)
        bucket.create()

    for data in datasets:
        blob = bucket.blob(f"{bucket_folder}{data.split('..')[-1]}")
        try:
            blob.upload_from_filename(data)
            print("File {} uploaded to {}.".format(data, bucket_folder))
        except Exception as e:
            print(e)


if __name__ == "__main__":
    upload_blob(project_id, bucket_name, bucket_folder, datasets)
