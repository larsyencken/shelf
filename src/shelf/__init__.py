import argparse
import hashlib
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Union

import boto3
import yaml
from dotenv import load_dotenv

load_dotenv()


BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / "data"
METADATA_DIR = BASE_DIR / "metadata"

BLACKLIST = [".DS_Store"]


def add(file_path: Union[str, Path], dataset_name: str, edit=False) -> str:
    file_path = Path(file_path)
    dataset_name = _process_dataset_name(dataset_name)

    print(f"Shelving: {file_path}")
    print(f"  CREATE   metadata/{dataset_name}.json")
    if file_path.is_dir():
        metadata = add_directory_to_shelf(file_path, dataset_name)
    else:
        metadata = add_file_to_shelf(file_path, dataset_name)

    # Save metadata record to YAML file
    metadata_file = save_metadata(metadata, dataset_name)

    # Open metadata file in interactive editor
    if edit and sys.stdout.isatty():
        open_in_editor(metadata_file)

    return dataset_name


def add_directory_to_shelf(file_path: Path, dataset_name: str) -> dict:
    # copy directory to data/
    data_path = DATA_DIR / dataset_name
    data_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"  COPY     {file_path}/ --> {data_path.relative_to(BASE_DIR)}/")
    shutil.copytree(file_path, data_path)

    # shelve directory contents
    checksums = add_directory_to_s3(data_path)

    # Save manifest file
    manifest_file = data_path / "MANIFEST.yaml"
    with open(manifest_file, "w") as f:
        yaml.dump(checksums, f)

    # Upload manifest file to S3
    manifest_checksum = generate_checksum(manifest_file)
    add_to_s3(manifest_file, manifest_checksum)

    # Create metadata record
    metadata = {
        "dataset_name": dataset_name,
        "type": "directory",
        "manifest": manifest_checksum,
    }

    # Save metadata record to YAML file
    save_metadata(metadata, dataset_name)

    return metadata


def add_directory_to_s3(file_path):
    checksums = []
    for root, dirs, files in os.walk(file_path):
        for file in files:
            if file in BLACKLIST:
                continue

            file_full_path = os.path.join(root, file)
            checksum = generate_checksum(file_full_path)
            add_to_s3(file_full_path, checksum)
            checksums.append(
                {
                    "path": os.path.relpath(file_full_path, file_path),
                    "checksum": checksum,
                }
            )

    return sorted(checksums, key=lambda x: x["path"])


def add_file_to_shelf(file_path: Path, dataset_name: str) -> dict:
    # Generate checksum for the file
    checksum = generate_checksum(file_path)

    # Upload file to S3-compatible store
    add_to_s3(file_path, checksum)

    # Copy file to data directory
    copy_to_data_dir(file_path, dataset_name)

    # Create metadata record
    metadata = {
        "dataset_name": dataset_name,
        "checksum": checksum,
        "extension": os.path.splitext(file_path)[1],
    }
    return metadata


def generate_checksum(file_path: Union[str, Path]) -> str:
    sha256 = hashlib.sha256()

    with open(file_path, "rb") as f:
        for block in iter(lambda: f.read(4096), b""):
            sha256.update(block)

    return sha256.hexdigest()


def add_to_s3(file_path: Union[str, Path], checksum: str) -> None:
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
    )
    bucket_name = os.getenv("S3_BUCKET_NAME")
    dest_path = f"{checksum[:2]}/{checksum[2:4]}/{checksum}"
    print(f"  UPLOAD   {file_path} --> s3://{bucket_name}/{dest_path}")
    s3.upload_file(file_path, bucket_name, str(dest_path))


def save_metadata(metadata: dict, dataset_name: str) -> Path:
    metadata_dir = METADATA_DIR / dataset_name
    metadata_dir.parent.mkdir(parents=True, exist_ok=True)

    metadata_file = metadata_dir.with_suffix(".yaml")

    with open(metadata_file, "w") as f:
        yaml.dump(metadata, f)

    return metadata_file


def open_in_editor(file_path: Path) -> None:
    editor = os.getenv("EDITOR", "vim")
    subprocess.run([editor, file_path])


def copy_to_data_dir(file_path: Path, dataset_name: str) -> None:
    data_dir = DATA_DIR / dataset_name
    data_dir.parent.mkdir(parents=True, exist_ok=True)

    assert not Path(file_path).is_dir()

    data_file = data_dir.with_suffix(file_path.suffix)
    shutil.copy2(file_path, data_file)
    print(f"  COPY     {file_path} --> {data_file}")


def get(path: Optional[str] = None) -> None:
    datasets = walk_metadata_files()
    if path:
        regex = re.compile(path)
        datasets = [d for d in datasets if regex.search(str(d))]

    if not datasets:
        raise KeyError(f"No datasets found for path: {path}")

    for metadata_file in datasets:
        restore_dataset(metadata_file)


def walk_metadata_files() -> list[Path]:
    return [
        Path(root) / file
        for root, _, files in os.walk("metadata")
        for file in files
        if file.endswith('.yaml')
    ]


def restore_dataset(metadata_file: Path) -> None:
    with open(metadata_file, "r") as f:
        metadata = yaml.safe_load(f)

    dataset_name = metadata["dataset_name"]

    print(f"Restoring: {dataset_name}")

    data_dir = DATA_DIR / dataset_name
    data_dir.parent.mkdir(parents=True, exist_ok=True)

    if metadata.get("type") == "directory":
        restore_directory(metadata, data_dir)
    else:
        restore_file(metadata, data_dir)


def restore_file(metadata: dict, data_dir: Path) -> None:
    file_extension = metadata["extension"]
    dest_file = data_dir.with_suffix(file_extension)
    fetch_from_s3(metadata["checksum"], dest_file)


def restore_directory(metadata: dict, data_dir: Path):
    # make the parent directory
    dest_dir = data_dir
    dest_dir.mkdir(parents=True, exist_ok=True)

    # fetch the manifest into it
    manifest_file = dest_dir / "MANIFEST.yaml"
    fetch_from_s3(metadata["manifest"], manifest_file)

    # load its records
    with open(manifest_file, "r") as f:
        manifest = yaml.safe_load(f)

    # fetch each file it mentions
    for item in manifest:
        file_path = data_dir / item['path']
        
        # let's not shoot ourselves in the foot and go writing anywhere in the filesystem
        if not file_path.resolve().is_relative_to(dest_dir.resolve()):
            raise Exception(f'manifest contains path {item['path']} outside the destination directory {dest_dir}')

        fetch_from_s3(item["checksum"], file_path)


def fetch_from_s3(checksum: str, dest_path: Path) -> None:
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
    )
    bucket_name = os.getenv("S3_BUCKET_NAME")
    assert bucket_name
    s3_path = f"{checksum[:2]}/{checksum[2:4]}/{checksum}"
    print(f"  FETCH    s3://{bucket_name}/{s3_path} --> {dest_path.resolve().relative_to(BASE_DIR)}")
    s3.download_file(bucket_name, s3_path, str(dest_path))


def main():
    parser = argparse.ArgumentParser(
        description="Shelf a data file or directory by adding it in a content-addressable way to the S3-compatible store."
    )
    subparsers = parser.add_subparsers(dest="command")

    add_parser = subparsers.add_parser(
        "add", help="Add a data file or directory to the content store"
    )
    add_parser.add_argument(
        "file_path", type=str, help="Path to the data file or directory"
    )
    add_parser.add_argument(
        "dataset_name",
        type=str,
        help="Dataset name as a relative path of arbitrary size",
    )

    get_parser = subparsers.add_parser(
        "get", help="Fetch and unpack data from the content store"
    )
    get_parser.add_argument(
        "path",
        type=str,
        nargs="?",
        help="Optional regex to match against metadata path names",
    )

    args = parser.parse_args()

    if args.command == "add":
        return add(args.file_path, args.dataset_name)

    elif args.command == "get":
        return get(args.path)

    parser.print_help()


def _process_dataset_name(dataset_name: str) -> str:
    parts = dataset_name.split("/")
    last_part = parts[-1]
    if not re.match(r"\d{4}-\d{2}-\d{2}", last_part) and last_part != "latest":
        parts.append(datetime.today().strftime("%Y-%m-%d"))
    return "/".join(parts)


if __name__ == "__main__":
    main()
