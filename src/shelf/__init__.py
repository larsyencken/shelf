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


class Shelf:
    def __init__(self, config_file: Optional[Path] = None):
        self.config = ShelfConfig.detect(config_file)

    def add(self, file_path: Union[str, Path], dataset_name: str, edit=False) -> str:
        file_path = Path(file_path)
        dataset_name = self._process_dataset_name(dataset_name)

        print(f"Shelving: {file_path}")
        print(f"  CREATE   data/{dataset_name}.meta.json")
        if file_path.is_dir():
            metadata = self.add_directory_to_shelf(file_path, dataset_name)
        else:
            metadata = self.add_file_to_shelf(file_path, dataset_name)

        # Save metadata record to YAML file
        metadata_file = self.save_metadata(metadata, dataset_name)

        # Open metadata file in interactive editor
        if edit and sys.stdout.isatty():
            self.open_in_editor(metadata_file)

        # Append data path to .gitignore
        gitignore = self.config.base_dir / ".gitignore"
        append_to_gitignore(gitignore, metadata)

        return dataset_name

    def add_directory_to_shelf(self, file_path: Path, dataset_name: str) -> dict:
        # copy directory to data/
        data_path = self.config.abs_data_dir / dataset_name
        data_path.parent.mkdir(parents=True, exist_ok=True)
        print(
            f"  COPY     {file_path}/ --> {data_path.relative_to(self.config.base_dir)}/"
        )
        shutil.copytree(file_path, data_path)

        # shelve directory contents
        checksums = self.add_directory_to_s3(data_path)

        # Save manifest file
        manifest_file = data_path / "MANIFEST.yaml"
        with open(manifest_file, "w") as f:
            yaml.dump(checksums, f)

        # Upload manifest file to S3
        manifest_checksum = self.generate_checksum(manifest_file)
        self.add_to_s3(manifest_file, manifest_checksum)

        # Create metadata record
        metadata = {
            "dataset_name": dataset_name,
            "type": "directory",
            "manifest": manifest_checksum,
        }

        # Save metadata record to YAML file
        self.save_metadata(metadata, dataset_name)

        return metadata

    def add_directory_to_s3(self, file_path):
        checksums = []
        for root, dirs, files in os.walk(file_path):
            for file in files:
                if file in BLACKLIST:
                    continue

                file_full_path = os.path.join(root, file)
                checksum = self.generate_checksum(file_full_path)
                self.add_to_s3(file_full_path, checksum)
                checksums.append(
                    {
                        "path": os.path.relpath(file_full_path, file_path),
                        "checksum": checksum,
                    }
                )

        return sorted(checksums, key=lambda x: x["path"])

    def add_file_to_shelf(self, file_path: Path, dataset_name: str) -> dict:
        # Generate checksum for the file
        checksum = self.generate_checksum(file_path)

        # Upload file to S3-compatible store
        self.add_to_s3(file_path, checksum)

        # Copy file to data directory
        self.copy_to_data_dir(file_path, dataset_name)

        # Create metadata record
        metadata = {
            "type": "file",
            "dataset_name": dataset_name,
            "checksum": checksum,
            "extension": os.path.splitext(file_path)[1],
        }
        return metadata

    def generate_checksum(self, file_path: Union[str, Path]) -> str:
        sha256 = hashlib.sha256()

        with open(file_path, "rb") as f:
            for block in iter(lambda: f.read(4096), b""):
                sha256.update(block)

        return sha256.hexdigest()

    def add_to_s3(self, file_path: Union[str, Path], checksum: str) -> None:
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

    def save_metadata(self, metadata: dict, dataset_name: str) -> Path:
        metadata_dir = self.config.abs_data_dir / dataset_name
        metadata_dir.parent.mkdir(parents=True, exist_ok=True)

        metadata_file = metadata_dir.with_suffix(".meta.yaml")

        with open(metadata_file, "w") as f:
            yaml.dump(metadata, f)

        return metadata_file

    def open_in_editor(self, file_path: Path) -> None:
        editor = os.getenv("EDITOR", "vim")
        subprocess.run([editor, file_path])

    def copy_to_data_dir(self, file_path: Path, dataset_name: str) -> None:
        data_dir = self.config.abs_data_dir / dataset_name
        data_dir.parent.mkdir(parents=True, exist_ok=True)

        assert not Path(file_path).is_dir()

        data_file = data_dir.with_suffix(file_path.suffix)
        shutil.copy2(file_path, data_file)
        print(
            f"  COPY     {file_path} --> {data_file.relative_to(self.config.base_dir)}"
        )

    def get(self, path: Optional[str] = None, force: bool = False) -> None:
        datasets = self.walk_metadata_files()
        if path:
            regex = re.compile(path)
            datasets = [d for d in datasets if regex.search(str(d))]

        if not datasets:
            raise KeyError(f"No datasets found for path: {path}")

        for metadata_file in datasets:
            self.restore_dataset(metadata_file, force)

    def walk_metadata_files(self) -> list[Path]:
        return [
            Path(root) / file
            for root, _, files in os.walk(self.config.abs_data_dir)
            for file in files
            if file.endswith(".meta.yaml")
        ]

    def restore_dataset(self, metadata_file: Path, force: bool = False) -> None:
        with open(metadata_file, "r") as f:
            metadata = yaml.safe_load(f)

        dataset_name = metadata["dataset_name"]

        print(f"Restoring: {dataset_name}")

        data_path = Path(str(metadata_file).replace(".meta.yaml", ""))

        if metadata.get("type") == "directory":
            self.restore_directory(metadata, data_path, force)
        else:
            self.restore_file(metadata, data_path, force)

    def restore_file(self, metadata: dict, data_path: Path, force: bool = False) -> None:
        file_extension = metadata["extension"]
        dest_file = data_path.with_suffix(file_extension)
        if force or not dest_file.exists() or self.generate_checksum(dest_file) != metadata["checksum"]:
            self.fetch_from_s3(metadata["checksum"], dest_file)

    def restore_directory(self, metadata: dict, data_path: Path, force: bool = False):
        # fetch the manifest
        if force or not data_path.exists() or not self.is_directory_up_to_date(metadata, data_path):
            if data_path.exists():
                shutil.rmtree(data_path)
            data_path.mkdir()
            manifest_file = data_path / "MANIFEST.yaml"
            self.fetch_from_s3(metadata["manifest"], manifest_file)

            # load its records
            with open(manifest_file, "r") as f:
                manifest = yaml.safe_load(f)

            # fetch each file it mentions
            for item in manifest:
                file_path = data_path / item["path"]

                # let's not shoot ourselves in the foot and go writing anywhere in the filesystem
                if not file_path.resolve().is_relative_to(data_path.resolve()):
                    raise Exception(
                        f'manifest contains path {item["path"]} outside the destination directory {data_path}'
                    )

                self.fetch_from_s3(item["checksum"], file_path)

    def is_directory_up_to_date(self, metadata: dict, data_path: Path) -> bool:
        manifest_file = data_path / "MANIFEST.yaml"
        if not manifest_file.exists():
            return False

        if self.generate_checksum(manifest_file) != metadata['checksum']:
            return False

        with open(manifest_file, "r") as f:
            manifest = yaml.safe_load(f)

        for item in manifest:
            file_path = data_path / item["path"]
            if not file_path.exists() or self.generate_checksum(file_path) != item["checksum"]:
                return False

        return True

    def fetch_from_s3(self, checksum: str, dest_path: Path) -> None:
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
            f"  FETCH    s3://{bucket_name}/{s3_path} --> {dest_path.resolve().relative_to(self.config.base_dir)}"
        )
        s3.download_file(bucket_name, s3_path, str(dest_path))

    def list_datasets(self, regex: Optional[str] = None) -> None:
        metadata_files = self.walk_metadata_files()
        suffix = ".meta.yaml"
        dataset_names = [
            str(d.relative_to(self.config.abs_data_dir))[: -len(suffix)]
            for d in metadata_files
        ]

        if regex:
            pattern = re.compile(regex)
            dataset_names = [name for name in dataset_names if pattern.search(name)]

        for name in sorted(dataset_names):
            print(name)

    def __iter__(self):
        metadata_files = self.walk_metadata_files()
        for metadata_file in metadata_files:
            yield metadata_file

    def __getitem__(self, dataset_path: str):
        metadata_file = self.config.abs_data_dir / f"{dataset_path}.meta.yaml"
        if not metadata_file.exists():
            raise KeyError(f"No dataset found for path: {dataset_path}")
        with open(metadata_file, "r") as f:
            metadata = yaml.safe_load(f)
        return metadata

    def _process_dataset_name(self, dataset_name: str) -> str:
        parts = dataset_name.split("/")

        if self._is_valid_version(parts[-1]):
            if len(parts) == 1:
                raise Exception("a dataset must have a name as well as a version")

            # the final segment is a version, all good
            return dataset_name

        # add a version to the end
        parts.append(datetime.today().strftime("%Y-%m-%d"))

        return "/".join(parts)

    def _is_valid_version(self, version: str) -> bool:
        return bool(re.match(r"\d{4}-\d{2}-\d{2}", version)) or version == "latest"

    @classmethod
    def init(cls, path: Optional[Path] = None) -> "Shelf":
        if not path:
            path = Path(".")

        shelf_file = path / "shelf.yaml"
        if shelf_file.exists():
            print("Detected existing shelf.yaml file")
        else:
            print("Initializing a new shelf")
            print(f"  CREATE   {shelf_file}")
            with shelf_file.open("w") as ostream:
                yaml.dump({"version": 1, "data_dir": "data", "steps": []}, ostream)

        return cls(shelf_file)


@dataclass
class ShelfConfig:
    config_file: Path
    version: int
    data_dir: str
    steps: list[Union[str, dict]]

    @property
    def abs_data_dir(self) -> Path:
        return (self.config_file.parent / self.data_dir).resolve()

    @property
    def base_dir(self) -> Path:
        return self.config_file.parent.resolve()

    @staticmethod
    def detect(config_file: Optional[Path] = None) -> "ShelfConfig":
        if not config_file:
            config_file = Path("shelf.yaml")

        if not config_file.exists():
            raise FileNotFoundError("No shelf.yaml file found in the current directory")

        with config_file.open("r") as istream:
            config = yaml.safe_load(istream)

        schema = _load_schema()
        jsonschema.validate(config, schema)
        return ShelfConfig(config_file, **config)


def _load_schema() -> dict:
    with open(SCHEMA_PATH, "r") as istream:
        return yaml.safe_load(istream)


def append_to_gitignore(gitignore_path: Path, metadata: dict) -> None:
    if not gitignore_path.exists():
        print("  CREATE  .gitignore")

    relative_data_path = f"data/{metadata['dataset_name']}"
    if metadata["type"] == "file":
        relative_data_path += metadata["extension"]

    with open(gitignore_path, "a") as f:
        f.write(f"{relative_data_path}\n")
    print(f"  APPEND   {relative_data_path} to .gitignore")


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
    get_parser.add_argument(
        "--force",
        action="store_true",
        help="Re-fetch datasets regardless of their up-to-date status",
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

    if args.command == "init":
        Shelf.init()
        return

    shelf = Shelf()

    if args.command == "add":
        return shelf.add(args.file_path, args.dataset_name)

    elif args.command == "get":
        return shelf.get(args.path, args.force)

    elif args.command == "list":
        return shelf.list_datasets(args.regex)

    parser.print_help()
