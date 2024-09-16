import argparse
import os
import re
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Optional

import duckdb
from dotenv import load_dotenv

from shelf import steps
from shelf.core import Shelf
from shelf.exceptions import StepDefinitionError
from shelf.snapshots import Snapshot
from shelf.types import StepURI
from shelf.utils import add_to_gitignore, checksum_manifest, console

load_dotenv()


BLACKLIST = [".DS_Store"]

SCHEMA_PATH = Path(__file__).parent / "shelf-v1.schema.json"


def main():
    parser = argparse.ArgumentParser(
        description="Shelf a data file or directory by adding it in a content-addressable way to the S3-compatible store."
    )
    subparsers = parser.add_subparsers(dest="command")

    snapshot_parser = subparsers.add_parser(
        "snapshot", help="Add a data file or directory to the content store"
    )
    snapshot_parser.add_argument(
        "file_path", type=str, help="Path to the data file or directory"
    )
    snapshot_parser.add_argument(
        "dataset_name",
        type=str,
        help="Dataset name as a relative path of arbitrary size",
    )
    snapshot_parser.add_argument(
        "--edit",
        action="store_true",
        help="Edit the metadata file in an interactive editor.",
    )

    run_parser = subparsers.add_parser(
        "run", help="Execute any outstanding steps in the DAG"
    )
    run_parser.add_argument(
        "path",
        type=str,
        nargs="?",
        help="Optional regex to match against step names",
    )
    run_parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-build of steps",
    )
    run_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't execute, just print the steps that would be executed",
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
    list_parser.add_argument(
        "--paths",
        action="store_true",
        help="Return relative paths instead of URIs",
    )

    subparsers.add_parser(
        "init", help="Initialize the shelf with the necessary directories"
    )

    audit_parser = subparsers.add_parser(
        "audit", help="Audit the shelf metadata and validate the metadata of every step"
    )
    audit_parser.add_argument(
        "--fix",
        action="store_true",
        help="Fix the overall checksum for snapshot steps with snapshot_type of directory if it is wrong",
    )

    export_parser = subparsers.add_parser(
        "export-duckdb", help="Export tables to a DuckDB file"
    )
    export_parser.add_argument(
        "db_file", type=str, help="Path to the DuckDB file to export tables to"
    )

    new_table_parser = subparsers.add_parser(
        "new-table", help="Create a new table with optional dependencies"
    )
    new_table_parser.add_argument("table_path", type=str, help="Path to the new table")
    new_table_parser.add_argument(
        "dependencies", type=str, nargs="*", help="Optional dependencies for the table"
    )
    new_table_parser.add_argument(
        "--edit",
        action="store_true",
        help="Edit the metadata file in an interactive editor.",
    )

    args = parser.parse_args()

    if args.command == "init":
        return init_shelf()

    shelf = Shelf()

    if args.command == "snapshot":
        snapshot_to_shelf(Path(args.file_path), args.dataset_name, edit=args.edit)
        return

    elif args.command == "list":
        return list_steps_cmd(shelf, args.regex, args.paths)

    elif args.command == "run":
        return plan_and_run(shelf, args.path, args.force, args.dry_run)

    elif args.command == "audit":
        return audit_shelf(shelf, args.fix)

    elif args.command == "export-duckdb":
        return export_duckdb(shelf, args.db_file)

    elif args.command == "new-table":
        return new_table(shelf, args.table_path, args.dependencies, args.edit)

    parser.print_help()


def init_shelf() -> None:
    print("Initializing shelf")
    Shelf.init()


def snapshot_to_shelf(
    file_path: Path, dataset_name: str, edit: bool = False
) -> Snapshot:
    _check_s3_credentials()

    # ensure we are tagging a version on everything
    dataset_name = _maybe_add_version(dataset_name)

    # sanity check that it does not exist
    shelf = Shelf()
    proposed_uri = StepURI("snapshot", dataset_name)
    if proposed_uri in shelf.steps:
        raise ValueError(f"Dataset already exists in shelf: {proposed_uri}")

    # create and add to s3
    print(f"Creating {proposed_uri}")
    snapshot = Snapshot.create(file_path, dataset_name)

    # ensure that the data itself does not enter git
    add_to_gitignore(snapshot.path)

    if edit:
        subprocess.run(["vim", snapshot.metadata_path])

    shelf.steps[proposed_uri] = []
    shelf.save()

    return snapshot


def list_steps_cmd(
    shelf: Shelf, regex: Optional[str] = None, paths: bool = False
) -> None:
    for step in list_steps(shelf, regex, paths):
        print(step)


def list_steps(
    shelf: Shelf, regex: Optional[str] = None, paths: bool = False
) -> list[Path] | list[StepURI]:
    steps = sorted(shelf.steps)

    if regex:
        steps = [s for s in steps if re.search(regex, str(s))]

    if paths:
        steps = [s.rel_path for s in steps]

    return steps


def plan_and_run(
    shelf: Shelf,
    regex: Optional[str] = None,
    force: bool = False,
    dry_run: bool = False,
) -> None:
    # to help unit testing
    shelf.refresh()

    # XXX in the future, we could create a Plan object that explains why each step has
    #     been selected to be run, even down to the level of which checksums are out of
    #     date or which files are missing
    dag = shelf.steps
    if regex:
        dag = steps.prune_with_regex(dag, regex)

    if not force:
        dag = steps.prune_completed(dag)

    if not dag:
        print("Already up to date!")
        return

    steps.execute_dag(dag, dry_run=dry_run)


def export_duckdb(shelf: Shelf, db_file: str) -> None:
    # Ensure all tables are built
    plan_and_run(shelf)

    # Connect to DuckDB
    conn = duckdb.connect(db_file)

    for step in shelf.steps:
        if step.scheme == "table":
            table_name = step.path.replace("/", "_").replace("-", "").rsplit(".", 1)[0]
            table_path = (Path("data/tables") / step.path).with_suffix(".parquet")

            conn.execute(
                f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{table_path}')"
            )

    conn.close()


def audit_shelf(shelf: Shelf, fix: bool = False) -> None:
    # XXX in the future, we could automatically upgrade from one shelf format
    #     version to another, if there were breaking changes
    print(f"Auditing {len(shelf.steps)} steps")
    for step in shelf.steps:
        audit_step(step, fix)
        console.print(f"[blue]{'OK':>5}[/blue]   {step}")


def audit_step(step: StepURI, fix: bool = False) -> None:
    if step.scheme != "snapshot":
        return

    snapshot = Snapshot.load(step.path)
    if snapshot.snapshot_type != "directory":
        return

    manifest = snapshot.manifest
    if not manifest:
        raise StepDefinitionError(
            f"Snapshot {step} of type 'directory' is missing a manifest"
        )

    calculated_checksum = checksum_manifest(manifest)
    if calculated_checksum != snapshot.checksum:
        print(
            f"Checksum mismatch for {step}: {snapshot.checksum} != {calculated_checksum}"
        )
        if fix:
            print(f"Fixing checksum for {step}")
            snapshot.checksum = calculated_checksum
            snapshot.save()
        else:
            raise StepDefinitionError(
                f"Checksum mismatch for {step} of type 'directory'"
            )


def new_table(
    shelf: Shelf, table_path: str, dependencies: list[str], edit: bool = False
) -> None:
    table_uri = StepURI("table", table_path)
    if table_uri in shelf.steps:
        raise ValueError(f"Table already exists in shelf: {table_uri}")

    shelf.steps[table_uri] = [StepURI.parse(dep) for dep in dependencies]
    shelf.save()


def _maybe_add_version(dataset_name: str) -> str:
    parts = dataset_name.split("/")

    if _is_valid_version(parts[-1]):
        if len(parts) == 1:
            raise Exception("invalid dataset name")

        # the final segment is a version, all good
        return dataset_name

    # add a version to the end
    parts.append(datetime.today().strftime("%Y-%m-%d"))

    return "/".join(parts)


def _is_valid_version(version: str) -> bool:
    return bool(re.match(r"\d{4}-\d{2}-\d{2}", version)) or version == "latest"


def _check_s3_credentials() -> None:
    for key in [
        "S3_ACCESS_KEY",
        "S3_SECRET_KEY",
        "S3_ENDPOINT_URL",
        "S3_BUCKET_NAME",
    ]:
        if key not in os.environ:
            raise ValueError(f"Missing S3 credentials -- please set {key} in .env")
