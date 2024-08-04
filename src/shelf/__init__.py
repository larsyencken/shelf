import argparse
import re
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

from shelf import steps
from shelf.core import Shelf
from shelf.exceptions import StepDefinitionError
from shelf.snapshots import Snapshot
from shelf.types import StepURI
from shelf.utils import add_to_gitignore, checksum_manifest

load_dotenv()


BLACKLIST = [".DS_Store"]

SCHEMA_PATH = Path(__file__).parent / "shelf-v1.schema.json"


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
    add_parser.add_argument(
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

    args = parser.parse_args()

    if args.command == "init":
        return init_shelf()

    shelf = Shelf()

    if args.command == "snapshot":
        return snapshot_to_shelf(
            Path(args.file_path), args.dataset_name, edit=args.edit
        )

    elif args.command == "list":
        return list_steps_cmd(shelf, args.regex)

    elif args.command == "run":
        return plan_and_run(shelf, args.path, args.force, args.dry_run)

    elif args.command == "audit":
        return audit_shelf(shelf, args.fix)

    parser.print_help()


def init_shelf() -> None:
    print("Initializing shelf")
    Shelf.init()


def snapshot_to_shelf(
    file_path: Path, dataset_name: str, edit: bool = False
) -> Snapshot:
    # ensure we are tagging a version on everything
    dataset_name = _maybe_add_version(dataset_name)

    # sanity check that it does not exist
    shelf = Shelf()
    proposed_uri = StepURI("snapshot", dataset_name)
    if proposed_uri in shelf.steps:
        raise ValueError("Dataset already exists in shelf: {proposed_uri}")

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


def list_steps_cmd(shelf: Shelf, regex: Optional[str] = None) -> None:
    for step in list_steps(shelf, regex):
        print(step)


def list_steps(shelf: Shelf, regex: Optional[str] = None) -> list[StepURI]:
    steps = sorted(shelf.steps)

    if regex:
        steps = [s for s in steps if re.search(regex, str(s))]

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


def audit_shelf_cmd(shelf: Shelf, fix: bool = False) -> None:
    try:
        audit_shelf(shelf, fix)
    except Exception as e:
        print(e)
        exit(1)


def audit_shelf(shelf: Shelf, fix: bool = False) -> None:
    # XXX in the future, we could automatically upgrade from one shelf format
    #     version to another, if there were breaking changes
    for step in shelf.steps:
        audit_step(step, fix)


def audit_step(step: StepURI, fix: bool = False) -> None:
    if step.scheme != "snapshot":
        print(f"Skipping non-snapshot step {step}")
        return

    snapshot = Snapshot.load(step.path)
    if snapshot.snapshot_type != "directory":
        print(f"Skipping non-directory snapshot {step}")
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
