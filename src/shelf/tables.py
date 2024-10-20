import os
import subprocess
import sys
from collections import Counter
from pathlib import Path
from typing import Iterator

import duckdb
import jsonschema
import polars as pl

from shelf.paths import SNAPSHOT_DIR, TABLE_DIR, TABLE_SCRIPT_DIR
from shelf.schemas import TABLE_SCHEMA
from shelf.snapshots import Snapshot
from shelf.types import Manifest, StepURI
from shelf.utils import checksum_file, load_yaml, print_op, save_yaml


def is_completed(uri: StepURI, deps: list[StepURI]) -> bool:
    assert uri.scheme == "table"

    # the easy case; is it missing?
    if (
        not (TABLE_DIR / f"{uri.path}.parquet").exists()
        or not _metadata_path(uri).exists()
    ):
        return False

    # it's there, but is it up to date? check the manifest
    metadata = load_yaml(_metadata_path(uri))
    input_manifest = metadata["input_manifest"]

    # we should spot every dependency in the manifest
    for dep in deps:
        dep_meta = _metadata_path(dep).as_posix()
        if dep_meta not in input_manifest:
            return False

    # the dependencies should also be up to date
    for path, checksum in input_manifest.items():
        if not Path(path).exists() or checksum != checksum_file(path):
            return False

    return True


def build_table(uri: StepURI, dependencies: list[StepURI]) -> None:
    assert uri.scheme == "table"
    command = _generate_build_command(uri, dependencies)
    _exec_command(uri, command)
    _gen_metadata(uri, dependencies)


def _generate_build_command(uri: StepURI, dependencies: list[StepURI]) -> list[Path]:
    executable = _get_executable(uri)

    cmd = [executable]
    for dep in dependencies:
        cmd.append(_dependency_path(dep))

    dest_path = TABLE_DIR / f"{uri.path}.parquet"
    cmd.append(dest_path)

    return cmd


def _dependency_path(uri: StepURI) -> Path:
    if uri.scheme == "snapshot":
        return Snapshot.load(uri.path).path

    elif uri.scheme == "table":
        return TABLE_DIR / f"{uri.path}.parquet"
    else:
        raise ValueError(f"Unknown scheme {uri.scheme}")


def _is_valid_script(script: Path) -> bool:
    return script.is_file() and os.access(script, os.X_OK)


def _exec_command(uri: StepURI, command: list[Path]) -> None:
    print_op("EXECUTE", command[0])
    dest_path = command[-1]
    if dest_path.exists():
        dest_path.unlink()
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    if command[0].suffix == ".sql":
        _exec_sql_command(uri, command)
    else:
        _exec_python_command(uri, command)

    if not dest_path.exists():
        raise Exception(f"Table step {uri} did not generate the expected {dest_path}")


def _exec_python_command(uri: StepURI, command: list[Path]) -> None:
    command_s = [sys.executable] + [str(p.resolve()) for p in command]
    subprocess.run(command_s, check=True)


def _exec_sql_command(uri: StepURI, command: list[Path]) -> None:
    sql_file = command[0]
    output_file = command[-1]

    template_vars = _simplify_dependency_names(command[1:-1])

    with open(sql_file, "r") as f:
        sql = f.read().format(**template_vars)

    con = duckdb.connect(database=":memory:")
    sql = f"CREATE TEMPORARY TABLE data AS ({sql})"
    try:
        con.execute(sql)

    except duckdb.ParserException as e:
        raise ValueError(f"Error executing the following SQL\n\n{sql}\n\n{e}")

    except duckdb.BinderException as e:
        raise ValueError(f"Error executing the following SQL\n\n{sql}\n\n{e}")

    con.execute(f"COPY data TO '{output_file}' (FORMAT 'parquet')")


def _generate_candidate_names(dep: Path) -> Iterator[str]:
    parts = dep.parts
    name = parts[-2]
    yield name

    candidates = [name]
    for p in reversed(parts[:-2]):
        name = f"{p}_{name}"
        yield name

    version = parts[-1].replace("-", "")
    candidates.append(f"{name}_{version}")
    while True:
        yield name


def _simplify_dependency_names(deps: list[Path]) -> dict[str, Path]:
    mapping = {}

    to_map = {d: iter(_generate_candidate_names(d)) for d in deps}

    frontier = {d: next(to_map[d]) for d in deps}
    duplicates = {k for k, v in Counter(frontier.values()).items() if v >= 2}

    for d, name in list(frontier.items()):
        if d not in duplicates:
            mapping[name] = d
            del frontier[d]

    last_duplicates = duplicates
    while duplicates:
        frontier = {d: next(to_map[d]) for d in list(frontier)}
        duplicates = {k for k, v in Counter(frontier.values()).items() if v >= 2}
        if duplicates == last_duplicates:
            raise Exception(f"infinite loop resolving dependencies: {deps}")

        for d, name in list(frontier.items()):
            if d not in duplicates:
                mapping[name] = d
                del frontier[d]

    return mapping


def _metadata_path(uri: StepURI) -> Path:
    if uri.scheme == "snapshot":
        return (SNAPSHOT_DIR / uri.path).with_suffix(".meta.yaml")

    elif uri.scheme == "table":
        return (TABLE_DIR / f"{uri.path}.parquet").with_suffix(".meta.yaml")

    else:
        raise ValueError(f"Unknown scheme {uri.scheme}")


def _gen_metadata(uri: StepURI, dependencies: list[StepURI]) -> None:
    dest_path = _metadata_path(uri)
    metadata = {
        "uri": str(uri),
        "version": 1,
        "checksum": checksum_file(TABLE_DIR / f"{uri.path}.parquet"),
        "input_manifest": _generate_input_manifest(uri, dependencies),
    }

    if len(dependencies) == 1:
        # inherit metadata from the dependency
        dep_metadata_path = _metadata_path(dependencies[0])
        dep_metadata = load_yaml(dep_metadata_path)
        for field in [
            "name",
            "source_name",
            "source_url",
            "date_accessed",
            "access_notes",
        ]:
            if field in dep_metadata:
                metadata[field] = str(dep_metadata[field])

    metadata["schema"] = _infer_schema(uri)

    jsonschema.validate(metadata, TABLE_SCHEMA)

    columns = list(metadata["schema"].keys())
    if not any(col.startswith("dim_") for col in columns):
        # we have not yet written this metadata, so the step is not yet complete
        raise ValueError(
            f"Table {uri} does not have any dimension columns prefixed with dim_, found: {columns}"
        )

    save_yaml(metadata, dest_path)


def _generate_input_manifest(uri: StepURI, dependencies: list[StepURI]) -> Manifest:
    manifest = {}

    # add the script we used to generate the table
    executable = _get_executable(uri)
    manifest[str(executable)] = checksum_file(executable)

    # add every dependency's metadata file; that file includes a checksum of its data,
    # so we cover both data and metadata this way
    for dep in dependencies:
        dep_metadata_file = _metadata_path(dep)
        manifest[str(dep_metadata_file)] = checksum_file(dep_metadata_file)

    return manifest


def _infer_schema(uri: StepURI) -> dict[str, str]:
    data_path = TABLE_DIR / f"{uri.path}.parquet"
    df = pl.read_parquet(data_path)
    return {col: str(dtype) for col, dtype in df.schema.items()}


def _get_executable(uri: StepURI, check: bool = True) -> Path:
    base = TABLE_SCRIPT_DIR / uri.path

    for exec_base in [base, base.parent]:
        py_script = exec_base.with_suffix(".py")
        sql_script = exec_base.with_suffix(".sql")

        if py_script.exists():
            if check and not _is_valid_script(py_script):
                raise Exception(f"Missing execute permissions on {py_script}")

            return py_script

        elif sql_script.exists():
            return sql_script

    else:
        raise FileNotFoundError(f"Could not find script for {uri}")


def add_placeholder_script(uri: StepURI) -> Path:
    script_path = _get_executable(uri, check=False)
    if script_path.exists():
        raise ValueError(f"Script already exists: {script_path}")

    script_path.parent.mkdir(parents=True, exist_ok=True)
    content = """#!/usr/bin/env python3
import sys
import polars as pl

data = {
    "a": [1, 1, 3],
    "b": [2, 3, 5],
    "c": [3, 4, 6]
}

df = pl.DataFrame(data)

output_file = sys.argv[-1]
df.write_parquet(output_file)
"""

    script_path.write_text(content)
    script_path.chmod(0o755)

    return script_path
