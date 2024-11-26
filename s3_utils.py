import boto3
import os
import fnmatch


aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]

s3 = boto3.resource(
    "s3",
    endpoint_url="https://s3-west.nrp-nautilus.io/",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
)
bucket = s3.Bucket("braingeneersdev")

def download_files(*files):
    # connect to s3
    for file in files:
        directory = os.path.dirname(file)
        if not os.path.exists(directory):
            os.makedirs(directory)
        print(f"Downloading from jgf folder, if you don't want to do that use download_single_file instead")
        s3_path = os.path.join("jgf", file.split("/")[-2], file.split("/")[-1])

        # check if file exists locally otherwise download
        if not os.path.exists(file):
            print(f"Downloading {file} from s3")
            bucket.download_file(s3_path, file)
        else:
            print(f"{file} already exists locally, skipping download")


def download_checkpoint(key, local_path):
    if not os.path.exists(local_path):
        print(f"Downloading {key} from s3")
        bucket.download_file(key, local_path)

def download_single_file(key, local_path):
    if not os.path.exists(local_path):
        print(f"Downloading {key} from s3")
        bucket.download_file(key, local_path)
    else:
        print(f"{local_path} already exists locally, skipping download")

def upload_files(prefix, *files):
    for file in files:
        s3_path = os.path.join(f"jlehrer/benchmarking/{prefix}/", file.split("/")[-2], file.split("/")[-1])
        s3_path = os.path.normpath(s3_path)
        print(f"Uploading {file} to {s3_path}")
        bucket.upload_file(file, s3_path)

def list_files(prefix: str, top_level_only=False) -> list[str]:
    s3 = boto3.client(
        "s3",
        endpoint_url="https://s3-west.nrp-nautilus.io/",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    matching_files = []
    prefix = prefix.rstrip("*")  # Remove trailing "*" if present

    paginator = s3.get_paginator('list_objects_v2')
    if top_level_only:
        pages = paginator.paginate(Bucket='braingeneersdev', Prefix=prefix, Delimiter='/')
        for result in pages:
            for prefix in result.get('CommonPrefixes'):
                matching_files.append(prefix.get('Prefix'))
    else:
        pages = paginator.paginate(Bucket='braingeneersdev', Prefix=prefix)
        for response in pages:
            if 'Contents' in response:
                for obj in response['Contents']:
                    matching_files.append(obj['Key'])

    return matching_files