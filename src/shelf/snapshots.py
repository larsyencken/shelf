#
#  snapshots.py
#
#  Adding and removing snapshots from the Shelf.
#


import os
import shutil
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal, Optional, Union

import boto3
import jsonschema
import yaml

from shelf.paths import BASE_DIR, SNAPSHOT_DIR
from shelf.schemas import METADATA_SCHEMA
from shelf.types import Checksum, DatasetName, FileName, Manifest, StepURI
from shelf.utils import checksum_file, checksum_folder, checksum_manifest, print_op


@dataclass
class Snapshot:
    uri: StepURI
    snapshot_type: Literal["file", "directory"]
    checksum: Checksum
    version: int = 1

    manifest: Optional[Manifest] = None
    extension: Optional[str] = None

    name: Optional[str] = None
    source_name: Optional[str] = None
    source_url: Optional[str] = None
    date_accessed: Optional[str] = None

    @property
    def path(self):
        if self.snapshot_type == "file":
            return SNAPSHOT_DIR / f"{self.uri.path}{self.extension}"

        elif self.snapshot_type == "directory":
            return SNAPSHOT_DIR / self.uri.path

        raise ValueError(f"Unknown snapshot type: {self.snapshot_type}")

    @property
    def metadata_path(self) -> Path:
        return (SNAPSHOT_DIR / self.uri.path).with_suffix(".meta.yaml")

    @staticmethod
    def load(path: str) -> "Snapshot":
        metadata_file = (SNAPSHOT_DIR / path).with_suffix(".meta.yaml")

        with metadata_file.open("r") as f:
            metadata = yaml.safe_load(f)
            jsonschema.validate(metadata, METADATA_SCHEMA)

        metadata["uri"] = StepURI.parse(metadata["uri"])

        return Snapshot(**metadata)

    @staticmethod
    def create(local_path: Path, dataset_name: str) -> "Snapshot":
        if local_path.is_dir():
            return Snapshot.create_from_directory(local_path, dataset_name)
        else:
            return Snapshot.create_from_file(local_path, dataset_name)

    @staticmethod
    def create_from_directory(
        local_path: Path, dataset_name: DatasetName
    ) -> "Snapshot":
        data_path = SNAPSHOT_DIR / dataset_name

        # copy directory to data/snapshots/...
        copy_dir(local_path, data_path)

        # upload to s3
        manifest = add_directory_to_s3(data_path)
        checksum = checksum_manifest(manifest)

        # Create metadata record
        snapshot = Snapshot(
            uri=StepURI("snapshot", dataset_name),
            checksum=checksum,
            snapshot_type="directory",
            manifest=manifest,
        )
        snapshot.save()

        return snapshot

    def save(self):
        # prep the metadata record
        record = self.to_dict()
        jsonschema.validate(record, METADATA_SCHEMA)

        if not self.metadata_path.parent.exists():
            print_op("CREATE", self.metadata_path)
        else:
            print_op("UPDATE", self.metadata_path)

        self.metadata_path.parent.mkdir(parents=True, exist_ok=True)
        with self.metadata_path.open("w") as f:
            yaml.safe_dump(record, f)

    def to_dict(self) -> dict:
        record = asdict(self)
        record["uri"] = str(self.uri)
        for k in [k for k, v in record.items() if v is None]:
            del record[k]

        return record

    @staticmethod
    def create_from_file(local_path: Path, dataset_name: DatasetName) -> "Snapshot":
        # first we checksum
        checksum = checksum_file(local_path)

        # then copy it over right away, as a convenience
        data_path = (SNAPSHOT_DIR / dataset_name).with_suffix(local_path.suffix)
        copy_file(local_path, data_path)

        # it tells us the s3 path to store it at
        add_to_s3(data_path, checksum)

        # then save the metadata record
        snapshot = Snapshot(
            uri=StepURI("snapshot", dataset_name),
            checksum=checksum,
            snapshot_type="file",
            extension=local_path.suffix,
        )
        snapshot.save()

        return snapshot

    def is_up_to_date(self):
        if self.snapshot_type == "file":
            return self.path.exists() and self.checksum == checksum_file(self.path)

        elif self.snapshot_type == "directory":
            return self.path.is_dir() and self.checksum == checksum_manifest(
                checksum_folder(self.path)
            )

        raise ValueError(f"Unknown snapshot type: {self.snapshot_type}")

    def fetch(self) -> None:
        if self.snapshot_type == "file":
            fetch_from_s3(self.checksum, self.path)
            return

        elif self.snapshot_type == "directory":
            assert self.manifest is not None
            for file_name, checksum in self.manifest.items():
                fetch_from_s3(checksum, self.path / file_name)
            return

        raise ValueError(f"Unknown snapshot type: {self.snapshot_type}")


def add_directory_to_s3(file_path: Path) -> dict[FileName, Checksum]:
    checksums = checksum_folder(file_path)
    for file_name, checksum in checksums.items():
        add_to_s3(file_path / file_name, checksum)

    return checksums


def add_to_s3(file_path: Union[str, Path], checksum: Checksum) -> None:
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
    )
    bucket_name = os.getenv("S3_BUCKET_NAME")
    dest_path = f"{checksum[:2]}/{checksum[2:4]}/{checksum}"
    print_op("UPLOAD", f"{file_path} --> s3://{bucket_name}/{dest_path}")
    s3.upload_file(file_path, bucket_name, str(dest_path))


def open_in_editor(self, file_path: Path) -> None:
    editor = os.getenv("EDITOR", "vim")
    subprocess.run([editor, file_path])


def copy_file(local_path: Path, data_path: Path) -> None:
    assert not Path(local_path).is_dir()

    data_path.parent.mkdir(parents=True, exist_ok=True)

    print_op("COPY", f"{local_path} --> {data_path.relative_to(BASE_DIR)}")
    shutil.copy(local_path, data_path)


def copy_dir(local_path: Path, data_path: Path) -> None:
    assert local_path.is_dir()

    data_path.parent.mkdir(parents=True, exist_ok=True)

    print_op("COPY", f"{local_path}/ --> {data_path.relative_to(BASE_DIR)}/")
    shutil.copytree(local_path, data_path)


def is_completed(uri: StepURI) -> bool:
    assert uri.scheme == "snapshot"
    return Snapshot.load(uri.path).is_up_to_date()


def fetch_from_s3(checksum: Checksum, dest_path: Path) -> None:
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    s3_path = f"{checksum[:2]}/{checksum[2:4]}/{checksum}"
    download_file(s3_path, dest_path)


def download_file(s3_path: str, dest_path: Path) -> None:
    s3 = s3_client()

    bucket_name = os.environ["S3_BUCKET_NAME"]
    dest_path_rel = dest_path.resolve().relative_to(BASE_DIR.resolve())

    print_op(
        "DOWNLOAD",
        f"s3://{bucket_name}/{s3_path} --> {dest_path_rel}",
    )

    s3.download_file(bucket_name, s3_path, str(dest_path))


def s3_client():
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
    )
    return s3
