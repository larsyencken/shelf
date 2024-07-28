import argparse
import hashlib
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Union

import boto3
import jsonschema
import yaml
from dotenv import load_dotenv

load_dotenv()


BLACKLIST = [".DS_Store"]

SCHEMA_PATH = Path(__file__).parent / "shelf-v1.schema.json"


def add(file_path: Union[str, Path], dataset_name: str, edit=False) -> str:
    config = detect_shelf_config()
    file_path = Path(file_path)
    dataset_name = _process_dataset_name(dataset_name)

    print(f"Shelving: {file_path}")
    print(f"  CREATE   data/{dataset_name}.meta.json")
    if file_path.is_dir():
        metadata = add_directory_to_shelf(config, file_path, dataset_name)
    else:
        metadata = add_file_to_shelf(config, file_path, dataset_name)

    # Save metadata record to YAML file
    metadata_file = save_metadata(config, metadata, dataset_name)

    # Open metadata file in interactive editor
    if edit and sys.stdout.isatty():
        open_in_editor(metadata_file)

    # Append data path to .gitignore
    append_to_gitignore(config, metadata, dataset_name)

    return dataset_name


def add_directory_to_shelf(
    config: "ShelfConfig", file_path: Path, dataset_name: str
) -> dict:
    # copy directory to data/
    data_path = config.abs_data_dir / dataset_name
    data_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"  COPY     {file_path}/ --> {data_path.relative_to(config.base_dir)}/")
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
    save_metadata(config, metadata, dataset_name)

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


def add_file_to_shelf(config, file_path: Path, dataset_name: str) -> dict:
    # Generate checksum for the file
    checksum = generate_checksum(file_path)

    # Upload file to S3-compatible store
    add_to_s3(file_path, checksum)

    # Copy file to data directory
    copy_to_data_dir(config, file_path, dataset_name)

    # Create metadata record
    metadata = {
        "type": "file",
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


def save_metadata(config: "ShelfConfig", metadata: dict, dataset_name: str) -> Path:
    metadata_dir = config.abs_data_dir / dataset_name
    metadata_dir.parent.mkdir(parents=True, exist_ok=True)

    metadata_file = metadata_dir.with_suffix(".meta.yaml")

    with open(metadata_file, "w") as f:
        yaml.dump(metadata, f)

    return metadata_file


def open_in_editor(file_path: Path) -> None:
    editor = os.getenv("EDITOR", "vim")
    subprocess.run([editor, file_path])


def copy_to_data_dir(config, file_path: Path, dataset_name: str) -> None:
    data_dir = config.abs_data_dir / dataset_name
    data_dir.parent.mkdir(parents=True, exist_ok=True)

    assert not Path(file_path).is_dir()

    data_file = data_dir.with_suffix(file_path.suffix)
    shutil.copy2(file_path, data_file)
    print(f"  COPY     {file_path} --> {data_file.relative_to(config.base_dir)}")


def get(path: Optional[str] = None) -> None:
    config = detect_shelf_config()
    datasets = walk_metadata_files(config)
    if path:
        regex = re.compile(path)
        datasets = [d for d in datasets if regex.search(str(d))]

    if not datasets:
        raise KeyError(f"No datasets found for path: {path}")

    for metadata_file in datasets:
        restore_dataset(metadata_file)


def walk_metadata_files(config: "ShelfConfig") -> list[Path]:
    return [
        Path(root) / file
        for root, _, files in os.walk(config.abs_data_dir)
        for file in files
        if file.endswith(".meta.yaml")
    ]


def restore_dataset(metadata_file: Path) -> None:
    with open(metadata_file, "r") as f:
        metadata = yaml.safe_load(f)

    dataset_name = metadata["dataset_name"]

    print(f"Restoring: {dataset_name}")

    data_path = Path(str(metadata_file).replace(".meta.yaml", ""))
    config = detect_shelf_config()

    if metadata.get("type") == "directory":
        restore_directory(config, metadata, data_path)
    else:
        restore_file(config, metadata, data_path)


def restore_file(config: "ShelfConfig", metadata: dict, data_path: Path) -> None:
    file_extension = metadata["extension"]
    dest_file = data_path.with_suffix(file_extension)
    fetch_from_s3(config, metadata["checksum"], dest_file)


def restore_directory(config: "ShelfConfig", metadata: dict, data_path: Path):
    # fetch the manifest
    data_path.mkdir()
    manifest_file = data_path / "MANIFEST.yaml"
    fetch_from_s3(config, metadata["manifest"], manifest_file)

    # load its records
    with open(manifest_file, "r") as f:
        manifest = yaml.safe_load(f)

    # fetch each file it mentions
    for item in manifest:
        file_path = data_path / item["path"]

        # let's not shoot ourselves in the foot and go writing anywhere in the filesystem
        if not file_path.resolve().is_relative_to(data_path.resolve()):
            raise Exception(
                f'manifest contains path {item['path']} outside the destination directory {data_path}'
            )

        fetch_from_s3(config, item["checksum"], file_path)


def fetch_from_s3(config, checksum: str, dest_path: Path) -> None:
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
    )
    bucket_name = os.getenv("S3_BUCKET_NAME")
    assert bucket_name
    s3_path = f"{checksum[:2]}/{checksum[2:4]}/{checksum}"
    print(
        f"  FETCH    s3://{bucket_name}/{s3_path} --> {dest_path.resolve().relative_to(config.base_dir)}"
    )
    s3.download_file(bucket_name, s3_path, str(dest_path))


def list_datasets(regex: Optional[str] = None) -> None:
    config = detect_shelf_config()
    metadata_files = walk_metadata_files(config)
    suffix = ".meta.yaml"
    dataset_names = [
        str(d.relative_to(config.abs_data_dir))[: -len(suffix)] for d in metadata_files
    ]

    if regex:
        pattern = re.compile(regex)
        dataset_names = [name for name in dataset_names if pattern.search(name)]

    for name in sorted(dataset_names):
        print(name)


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

    list_parser = subparsers.add_parser(
        "list", help="List all datasets in alphabetical order"
    )
    list_parser.add_argument(
        "regex",
        type=str,
        nargs="?",
        help="Optional regex to filter dataset names",
    )

    subparsers.add_parser(
        "init", help="Initialize the shelf with the necessary directories"
    )

    args = parser.parse_args()

    if args.command == "add":
        return add(args.file_path, args.dataset_name)

    elif args.command == "get":
        return get(args.path)

    elif args.command == "list":
        return list_datasets(args.regex)

    elif args.command == "init":
        return init()

    parser.print_help()


def _process_dataset_name(dataset_name: str) -> str:
    parts = dataset_name.split("/")

    if _is_valid_version(parts[-1]):
        if len(parts) == 1:
            raise Exception("a dataset must have a name as well as a version")

        # the final segment is a version, all good
        return dataset_name

    # add a version to the end
    parts.append(datetime.today().strftime("%Y-%m-%d"))

    return "/".join(parts)


def _is_valid_version(version: str) -> bool:
    return bool(re.match(r"\d{4}-\d{2}-\d{2}", version)) or version == "latest"


def init(path: Optional[Path] = None) -> None:
    if not path:
        path = Path(".")

    shelf_file = path / "shelf.yaml"
    if shelf_file.exists():
        print("Detected existing shelf.yaml file")
        return

    print("Initializing a new shelf")
    print(f"  CREATE   {shelf_file}")
    with shelf_file.open("w") as ostream:
        yaml.dump({"version": 1, "data_dir": "data", "steps": []}, ostream)


@dataclass
class ShelfConfig:
    config_file: Path
    version: int
    data_dir: str
    steps: list[Union[str, dict]]

    @property
    def abs_data_dir(self) -> Path:
        return self.config_file.parent / self.data_dir

    @property
    def base_dir(self) -> Path:
        return self.config_file.parent / self.data_dir


def detect_shelf_config() -> ShelfConfig:
    config_file, config = _find_shelf_config()
    schema = _load_schema()
    jsonschema.validate(config, schema)
    return ShelfConfig(config_file, **config)


def _find_shelf_config() -> tuple[Path, dict]:
    config_file = Path(".").resolve() / "shelf.yaml"
    if config_file.exists():
        with open(config_file, "r") as istream:
            return config_file, yaml.safe_load(istream)

    raise Exception("No shelf.yaml file found -- have you run shelf init?")


def _load_schema() -> dict:
    with open(SCHEMA_PATH, "r") as istream:
        return yaml.safe_load(istream)


def append_to_gitignore(
    config: "ShelfConfig", metadata: dict, dataset_name: str
) -> None:
    gitignore_path = config.config_file.parent / ".gitignore"
    if not gitignore_path.exists():
        print("  CREATE  .gitignore")

    relative_data_path = f"data/{dataset_name}"
    if metadata["type"] == "file":
        relative_data_path += metadata["extension"]

    with open(gitignore_path, "a") as f:
        f.write(f"{relative_data_path}\n")
    print(f"  APPEND   {relative_data_path} to .gitignore")
