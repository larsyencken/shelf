import os
import hashlib
import boto3
import yaml
from dotenv import load_dotenv
import argparse
from datetime import datetime
import shutil

load_dotenv()

def shelve_data_file(file_path: str, namespace: str, dataset: str, version: str = None) -> None:
    if version is None:
        version = datetime.today().strftime('%Y-%m-%d')

    # Generate checksum for the file
    checksum = generate_checksum(file_path)

    # Upload file to S3-compatible store
    upload_to_s3(file_path, checksum)

    # Create metadata record
    metadata = {
        'namespace': namespace,
        'dataset': dataset,
        'version': version,
        'checksum': checksum,
        'extension': os.path.splitext(file_path)[1]
    }

    # Save metadata record to YAML file
    save_metadata(metadata, namespace, dataset, version)

    # Copy file to data directory
    copy_to_data_dir(file_path, namespace, dataset, version)

def generate_checksum(file_path: str) -> str:
    sha256 = hashlib.sha256()
    with open(file_path, 'rb') as f:
        for block in iter(lambda: f.read(4096), b''):
            sha256.update(block)
    return sha256.hexdigest()

def upload_to_s3(file_path: str, checksum: str) -> None:
    s3 = boto3.client('s3')
    bucket_name = os.getenv('B2_BUCKET_NAME')
    s3.upload_file(file_path, bucket_name, checksum)

def save_metadata(metadata: dict, namespace: str, dataset: str, version: str) -> None:
    metadata_dir = os.path.join('metadata', namespace, dataset)
    os.makedirs(metadata_dir, exist_ok=True)
    metadata_file = os.path.join(metadata_dir, f'{version}.yaml')
    with open(metadata_file, 'w') as f:
        yaml.dump(metadata, f)

def copy_to_data_dir(file_path: str, namespace: str, dataset: str, version: str) -> None:
    data_dir = os.path.join('data', namespace, dataset)
    os.makedirs(data_dir, exist_ok=True)
    file_extension = os.path.splitext(file_path)[1]
    data_file = os.path.join(data_dir, f'{version}{file_extension}')
    shutil.copy2(file_path, data_file)

def main():
    parser = argparse.ArgumentParser(description='Shelve a data file by adding it in a content-addressable way to the S3-compatible store.')
    parser.add_argument('file_path', type=str, help='Path to the data file')
    parser.add_argument('namespace', type=str, help='Namespace for the data file')
    parser.add_argument('dataset', type=str, help='Dataset for the data file')
    parser.add_argument('version', type=str, nargs='?', help='Version of the data file (optional, defaults to today\'s date)')
    args = parser.parse_args()

    shelve_data_file(args.file_path, args.namespace, args.dataset, args.version)

if __name__ == '__main__':
    main()
