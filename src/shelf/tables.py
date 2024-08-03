import os
import subprocess
from pathlib import Path

import jsonschema
import polars as pl
import yaml

from shelf.paths import SNAPSHOT_DIR, TABLE_DIR, TABLE_SCRIPT_DIR
from shelf.schemas import TABLE_SCHEMA
from shelf.snapshots import Snapshot
from shelf.types import Manifest, StepURI
from shelf.utils import checksum_file


def build_table(uri: StepURI, dependencies: list[StepURI]) -> None:
    print(uri)
    assert uri.scheme == "table"
    command = _generate_build_command(uri, dependencies)
    _exec_command(uri, command)
    _gen_metadata(uri, dependencies)


def _generate_build_command(uri: StepURI, dependencies: list[StepURI]) -> list[Path]:
    executable = _get_executable(uri)

    cmd = [executable]
    for dep in dependencies:
        cmd.append(_dependency_path(dep))

    dest_path = TABLE_DIR / uri.path
    cmd.append(dest_path)

    return cmd


def _dependency_path(uri: StepURI) -> Path:
    if uri.scheme == "snapshot":
        return Snapshot.load(uri.path).path

    elif uri.scheme == "table":
        return TABLE_DIR / uri.path
    else:
        raise ValueError(f"Unknown scheme {uri.scheme}")


def _is_valid_script(script: Path) -> bool:
    return script.is_file() and os.access(script, os.X_OK)


def _exec_command(uri: StepURI, command: list[Path]) -> None:
    dest_path = command[-1]
    if dest_path.exists():
        dest_path.unlink()

    subprocess.run(command, check=True)

    if not dest_path.exists():
        raise Exception(f"Table step {uri} did not generate the expected {dest_path}")


def _metadata_path(uri: StepURI) -> Path:
    if uri.scheme == "snapshot":
        return (SNAPSHOT_DIR / uri.path).with_suffix(".meta.yaml")

    elif uri.scheme == "table":
        return (TABLE_DIR / uri.path).with_suffix(".meta.yaml")

    else:
        raise ValueError(f"Unknown scheme {uri.scheme}")


def _gen_metadata(uri: StepURI, dependencies: list[StepURI]) -> None:
    dest_path = _metadata_path(uri)
    metadata = {
        "uri": str(uri),
        "version": 1,
        "checksum": checksum_file(TABLE_DIR / uri.path),
        "input_manifest": _generate_input_manifest(uri, dependencies),
    }

    if len(dependencies) == 1:
        # inherit metadata from the dependency
        dep_metadata_path = _metadata_path(dependencies[0])
        dep_metadata = yaml.safe_load(dep_metadata_path.read_text())
        for field in [
            "name",
            "source_name",
            "source_url",
            "date_accessed",
            "access_notes",
        ]:
            if field in dep_metadata:
                metadata[field] = dep_metadata[field]

    metadata["schema"] = _infer_schema(uri)

    jsonschema.validate(metadata, TABLE_SCHEMA)

    if not any(col.startswith("dim_") for col in metadata["schema"]):
        # we have not yet written this metadata, so the step is not yet complete
        raise ValueError(
            f"Table {uri} does not have any dimension columns prefixed with dim_"
        )

    dest_path.write_text(yaml.safe_dump(metadata))


def _generate_input_manifest(uri: StepURI, dependencies: list[StepURI]) -> Manifest:
    manifest = {}

    # add the script we used to generate the table
    executable = _get_executable(uri)
    manifest[str(executable)] = checksum_file(executable)

    # add every dependency's metadata file; that file includes a checksum of its data,
    # so we cover both data and metadata this way
    for dep in dependencies:
        dep_metadata_file = _metadata_path(dep)
        manifest[str(dep)] = checksum_file(dep_metadata_file)

    return manifest


def is_completed(step: StepURI) -> bool:
    assert step.scheme == "table"

    # the easy case; is it missing?
    if not (TABLE_DIR / step.path).exists() or not _metadata_path(step).exists():
        return False

    # it's there, but is it up to date? check the manifest
    metadata = yaml.safe_load(_metadata_path(step).read_text())
    input_manifest = metadata["input_manifest"]
    for path, checksum in input_manifest.items():
        if checksum != checksum_file(path):
            return False

    return True


def _infer_schema(uri: StepURI) -> dict[str, str]:
    data_path = TABLE_DIR / uri.path
    suffix = data_path.suffix

    if suffix in [".csv", ".tsv"]:
        df = pl.read_csv(data_path, separator="\t" if suffix == ".tsv" else ",")

    elif suffix == ".jsonl":
        df = pl.read_ndjson(data_path)

    elif suffix == ".feather":
        df = pl.read_ipc(data_path)

    elif suffix == ".parquet":
        df = pl.read_parquet(data_path)

    else:
        raise ValueError("Unsupported file type")

    return {col: str(dtype) for col, dtype in df.schema.items()}


def _get_executable(uri: StepURI) -> Path:
    executable = (TABLE_SCRIPT_DIR / uri.path).with_suffix("")
    if not _is_valid_script(executable):
        executable = executable.parent
        if not _is_valid_script(executable):
            raise FileNotFoundError(f"No executable script found for table step {uri}")

    return executable
