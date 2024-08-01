import hashlib
from pathlib import Path
from typing import Any, Union

from shelf.types import Checksum, Manifest

IGNORE_FILES = {".DS_Store"}


def checksum_file(file_path: Union[str, Path]) -> Checksum:
    sha256 = hashlib.sha256()

    with open(file_path, "rb") as f:
        for block in iter(lambda: f.read(4096), b""):
            sha256.update(block)

    return sha256.hexdigest()


def checksum_folder(dir_path: Path) -> Manifest:
    manifest = {}
    # walk the subdirectory tree, adding relative path and checksums to the manifest
    for file_path in dir_path.rglob("*"):
        if file_path.is_file():
            if file_path.name in IGNORE_FILES:
                continue
            rel_path = file_path.relative_to(dir_path)
            manifest[str(rel_path)] = checksum_file(file_path)

    if not manifest:
        raise Exception(f'No files found in "{dir_path}" to checksum')

    return manifest


def checksum_manifest(manifest: Manifest) -> Checksum:
    sha256 = hashlib.sha256()

    for file_name, checksum in sorted(manifest.items()):
        sha256.update(file_name.encode())
        sha256.update(checksum.encode())

    return sha256.hexdigest()


def print_op(type_: str, message: Any) -> None:
    print(f"{type_:>15}   {message}")