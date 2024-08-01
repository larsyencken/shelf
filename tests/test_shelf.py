import os
import shutil
from pathlib import Path

import pytest
import yaml
from shelf import Shelf, list_steps, plan_and_run, snapshot_to_shelf, audit_shelf
from shelf.paths import BASE_DIR
from shelf.types import StepURI
from shelf.utils import checksum_folder  # noqa

BASE = Path(__file__).parent.parent


@pytest.fixture
def setup_test_environment(tmp_path):
    # Setup temporary environment for testing
    os.environ["S3_ACCESS_KEY"] = os.environ.get("TEST_ACCESS_KEY", "justtesting")
    os.environ["S3_SECRET_KEY"] = os.environ.get("TEST_SECRET_KEY", "justtesting")
    os.environ["S3_BUCKET_NAME"] = os.environ.get("TEST_BUCKET_NAME", "test")
    os.environ["S3_ENDPOINT_URL"] = os.environ.get(
        "TEST_ENDPOINT_URL", "http://localhost:9000"
    )

    # Create test directory and files
    test_dir = tmp_path / "test_dir"
    test_dir.mkdir()

    # Change to test directory
    os.chdir(test_dir)

    yield test_dir

    # Cleanup
    shutil.rmtree(test_dir)


def test_step_uri():
    uri = StepURI.parse("snapshot://test_namespace/test_dataset/2024-07-26")
    assert uri.scheme == "snapshot"
    assert uri.path == "test_namespace/test_dataset/2024-07-26"
    assert str(uri) == "snapshot://test_namespace/test_dataset/2024-07-26"


def test_path_variables_are_dynamic(setup_test_environment):
    tmp_path = setup_test_environment
    assert BASE_DIR.resolve().is_relative_to(tmp_path)


def test_add_file(setup_test_environment):
    tmp_path = setup_test_environment

    # configure test
    uri = StepURI.parse("snapshot://test_namespace/test_dataset/2024-07-26")
    data_file = (
        tmp_path
        / "data"
        / "snapshots"
        / "test_namespace"
        / "test_dataset"
        / "2024-07-26.txt"
    )
    metadata_file = (
        tmp_path
        / "data"
        / "snapshots"
        / "test_namespace"
        / "test_dataset"
        / "2024-07-26.meta.yaml"
    )
    gitignore_file = tmp_path / ".gitignore"
    shelf_yaml_file = tmp_path / "shelf.yaml"

    # create dummy file
    new_file = tmp_path / "file1.txt"
    new_file.write_text("Hello, World!")

    # add file to shelf
    os.chdir(tmp_path)
    shelf = Shelf.init()
    snapshot_to_shelf(new_file, uri.path)

    # check for data and metadata
    assert data_file.exists()
    assert metadata_file.exists()

    # check if data path is added to .gitignore
    assert gitignore_file.exists()
    with open(gitignore_file, "r") as f:
        assert f"data/snapshots/{uri.path}.txt\n" in f.read()

    # check if dataset name is added to shelf.yaml under steps
    assert shelf_yaml_file.exists()
    with open(shelf_yaml_file, "r") as f:
        shelf_yaml = yaml.safe_load(f)
        assert str(uri) in shelf_yaml["steps"]

    # re-fetch it from shelf
    data_file.unlink()
    shelf.refresh()
    plan_and_run(shelf, str(uri))
    assert data_file.exists()
    assert data_file.read_text() == "Hello, World!"


def test_checksum_folder(setup_test_environment):
    tmp_path = setup_test_environment

    # create dummy data
    local_data_dir = tmp_path / "example"
    local_data_dir.mkdir()
    (local_data_dir / "file1.txt").write_text("Hello, World!")
    (local_data_dir / "file2.txt").write_text("Hello, Cosmos!")

    # calculate checksums
    manifest = checksum_folder(local_data_dir)
    assert manifest == {
        "file1.txt": "dffd6021bb2bd5b0af676290809ec3a53191dd81c7f70a4b28688a362182986f",
        "file2.txt": "40efcea9db03adb126f27a0f339c595d1828a0713a789ea49d1ae67159d101e0",
    }


def test_shelve_directory(setup_test_environment):
    tmp_path = setup_test_environment

    # configure test
    path = "test_namespace/test_dataset/latest"
    data_path = tmp_path / "data/snapshots/" / path
    metadata_file = (tmp_path / "data/snapshots" / path).with_suffix(".meta.yaml")
    gitignore_file = tmp_path / ".gitignore"
    shelf_yaml_file = tmp_path / "shelf.yaml"

    # create dummy data
    local_data_dir = tmp_path / "example"
    local_data_dir.mkdir()
    (local_data_dir / "file1.txt").write_text("Hello, World!")
    (local_data_dir / "file2.txt").write_text("Hello, Cosmos!")

    # add to shelf
    os.chdir(tmp_path)
    shelf = Shelf.init()
    snapshot_to_shelf(local_data_dir, path)

    # check the right local files are created
    assert data_path.is_dir()
    assert metadata_file.exists()

    # check if manifest is present in metadata
    with open(metadata_file, "r") as f:
        metadata = yaml.safe_load(f)
        assert metadata.get("manifest")

    # check if data path is added to .gitignore
    assert gitignore_file.exists()
    with open(gitignore_file, "r") as f:
        assert f"{path}\n" in f.read()

    # check if dataset name is added to shelf.yaml under steps
    assert shelf_yaml_file.exists()
    with open(shelf_yaml_file, "r") as f:
        shelf_yaml = yaml.safe_load(f)
        assert f"snapshot://{path}" in shelf_yaml["steps"]

    # clear the data
    shutil.rmtree(data_path)

    # restore from shelf
    plan_and_run(shelf)

    # check it got restored
    assert data_path.is_dir()
    assert (data_path / "file1.txt").exists()
    assert (data_path / "file2.txt").exists()


def test_add_file_with_arbitrary_depth_namespace(setup_test_environment):
    tmp_path = setup_test_environment

    # configure test
    path = "a/b/c/2024-07-26"
    data_file = tmp_path / "data/snapshots" / "a" / "b" / "c" / "2024-07-26.txt"
    metadata_file = (
        tmp_path / "data/snapshots" / "a" / "b" / "c" / "2024-07-26.meta.yaml"
    )
    shelf_yaml_file = tmp_path / "shelf.yaml"

    # create dummy file
    new_file = tmp_path / "file1.txt"
    new_file.write_text("Hello, World!")

    # add file to shelf
    os.chdir(tmp_path)
    shelf = Shelf.init()
    snapshot_to_shelf(new_file, path)

    # check for data and metadata
    assert data_file.exists()
    assert metadata_file.exists()

    # check if dataset name is added to shelf.yaml under steps
    assert shelf_yaml_file.exists()
    with open(shelf_yaml_file, "r") as f:
        shelf_yaml = yaml.safe_load(f)
        assert f"snapshot://{path}" in shelf_yaml["steps"]

    # re-fetch it from shelf
    data_file.unlink()
    shelf.refresh()
    assert shelf.steps
    plan_and_run(shelf)
    assert data_file.exists()
    assert data_file.read_text() == "Hello, World!"


def test_shelve_directory_with_arbitrary_depth_namespace(setup_test_environment):
    tmp_path = setup_test_environment

    # configure test
    uri = StepURI.parse("snapshot://a/b/c/latest")
    data_path = tmp_path / "data/snapshots" / uri.path
    metadata_file = (tmp_path / "data/snapshots" / uri.path).with_suffix(".meta.yaml")
    shelf_yaml_file = tmp_path / "shelf.yaml"

    # create dummy data
    parent = tmp_path / "example"
    if parent.exists():
        shutil.rmtree(parent)
    parent.mkdir()
    (parent / "file1.txt").write_text("Hello, World!")
    (parent / "file2.txt").write_text("Hello, Cosmos!")

    # add to shelf
    os.chdir(tmp_path)
    shelf = Shelf.init()
    snapshot_to_shelf(parent, uri.path)

    # check the right local files are created
    assert data_path.is_dir()
    assert metadata_file.exists()

    # check if manifest is present in metadata
    with open(metadata_file, "r") as f:
        metadata = yaml.safe_load(f)
        assert "manifest" in metadata

    # check if dataset name is added to shelf.yaml under steps
    assert shelf_yaml_file.exists()
    with open(shelf_yaml_file, "r") as f:
        shelf_yaml = yaml.safe_load(f)
        assert str(uri) in shelf_yaml["steps"]

    # clear the data
    shutil.rmtree(data_path)

    # restore from shelf
    plan_and_run(shelf, str(uri))

    # check it got restored
    assert data_path.is_dir()
    assert (data_path / "file1.txt").exists()
    assert (data_path / "file2.txt").exists()


def test_list_datasets(setup_test_environment):
    tmp_path = setup_test_environment

    # configure test
    uri1 = StepURI.parse("snapshot://test_namespace/test_dataset1/2024-07-26")
    uri2 = StepURI.parse("snapshot://test_namespace/test_dataset2/2024-07-27")
    new_file1 = tmp_path / "file1.txt"
    new_file2 = tmp_path / "file2.txt"
    new_file1.write_text("Hello, World!")
    new_file2.write_text("Hello, Cosmos!")

    # add files to shelf
    os.chdir(tmp_path)
    shelf = Shelf.init()
    snapshot_to_shelf(new_file1, uri1.path)
    snapshot_to_shelf(new_file2, uri2.path)
    shelf.refresh()

    assert list_steps(shelf) == [uri1, uri2]


def test_list_datasets_with_regex(setup_test_environment):
    tmp_path = setup_test_environment

    # configure test
    uri1 = StepURI.parse("snapshot://test_namespace/test_dataset1/2024-07-26")
    uri2 = StepURI.parse("snapshot://test_namespace/test_dataset2/2024-07-27")
    new_file1 = tmp_path / "file1.txt"
    new_file2 = tmp_path / "file2.txt"
    new_file1.write_text("Hello, World!")
    new_file2.write_text("Hello, Cosmos!")

    # add files to shelf
    os.chdir(tmp_path)
    shelf = Shelf.init()
    snapshot_to_shelf(new_file1, uri1.path)
    snapshot_to_shelf(new_file2, uri2.path)

    shelf.refresh()
    assert list_steps(shelf, str(uri1)) == [uri1]


def test_get_only_out_of_date_datasets(setup_test_environment):
    tmp_path = setup_test_environment

    # configure test
    uri1 = StepURI("snapshot", "test_namespace/test_dataset1/2024-07-26")
    uri2 = StepURI("snapshot", "test_namespace/test_dataset2/2024-07-27")
    new_file1 = tmp_path / "file1.txt"
    new_file2 = tmp_path / "file2.txt"
    new_file1.write_text("Hello, World!")
    new_file2.write_text("Hello, Cosmos!")

    # add files to shelf
    os.chdir(tmp_path)
    shelf = Shelf.init()
    snapshot_to_shelf(new_file1, uri1.path)
    snapshot_to_shelf(new_file2, uri2.path)

    # modify one of the files to make it out of date
    data_file1 = (
        tmp_path
        / "data/snapshots"
        / "test_namespace"
        / "test_dataset1"
        / "2024-07-26.txt"
    )
    data_file1.write_text("Modified content")

    # restore datasets
    plan_and_run(shelf)

    # check that only the out-of-date dataset was fetched
    assert data_file1.read_text() == "Hello, World!"
    data_file2 = (
        tmp_path
        / "data/snapshots"
        / "test_namespace"
        / "test_dataset2"
        / "2024-07-27.txt"
    )
    assert data_file2.read_text() == "Hello, Cosmos!"


def test_get_with_force_option(setup_test_environment):
    tmp_path = setup_test_environment

    # configure test
    uri1 = StepURI.parse("snapshot://test_namespace/test_dataset1/2024-07-26")
    uri2 = StepURI.parse("snapshot://test_namespace/test_dataset2/2024-07-27")
    new_file1 = tmp_path / "file1.txt"
    new_file2 = tmp_path / "file2.txt"
    new_file1.write_text("Hello, World!")
    new_file2.write_text("Hello, Cosmos!")

    # add files to shelf
    os.chdir(tmp_path)
    shelf = Shelf.init()
    snapshot_to_shelf(new_file1, uri1.path)
    snapshot_to_shelf(new_file2, uri2.path)

    # modify one of the files to make it out of date
    data_file1 = (
        tmp_path
        / "data/snapshots"
        / "test_namespace"
        / "test_dataset1"
        / "2024-07-26.txt"
    )
    data_file1.write_text("Modified content")

    # restore datasets with --force option
    plan_and_run(shelf, force=True)

    # check that both datasets were fetched
    assert data_file1.read_text() == "Hello, World!"
    data_file2 = (
        tmp_path
        / "data/snapshots"
        / "test_namespace"
        / "test_dataset2"
        / "2024-07-27.txt"
    )
    assert data_file2.read_text() == "Hello, Cosmos!"


def test_audit_command(setup_test_environment):
    tmp_path = setup_test_environment

    # configure test
    path = "test_namespace/test_dataset/latest"
    data_path = tmp_path / "data/snapshots/" / path
    metadata_file = (tmp_path / "data/snapshots" / path).with_suffix(".meta.yaml")
    shelf_yaml_file = tmp_path / "shelf.yaml"

    # create dummy data
    local_data_dir = tmp_path / "example"
    local_data_dir.mkdir()
    (local_data_dir / "file1.txt").write_text("Hello, World!")
    (local_data_dir / "file2.txt").write_text("Hello, Cosmos!")

    # add to shelf
    os.chdir(tmp_path)
    shelf = Shelf.init()
    snapshot_to_shelf(local_data_dir, path)

    # check the right local files are created
    assert data_path.is_dir()
    assert metadata_file.exists()

    # check if manifest is present in metadata
    with open(metadata_file, "r") as f:
        metadata = yaml.safe_load(f)
        assert metadata.get("manifest")

    # modify the checksum in the metadata to simulate an incorrect checksum
    metadata["checksum"] = "incorrectchecksum"
    with open(metadata_file, "w") as f:
        yaml.safe_dump(metadata, f)

    # run the audit command
    from shelf import audit_shelf
    audit_shelf(shelf, fix=False)

    # check that the audit command detected the incorrect checksum
    with open(metadata_file, "r") as f:
        metadata = yaml.safe_load(f)
        assert metadata["checksum"] == "incorrectchecksum"

    # run the audit command with --fix option
    audit_shelf(shelf, fix=True)

    # check that the audit command fixed the incorrect checksum
    with open(metadata_file, "r") as f:
        metadata = yaml.safe_load(f)
        assert metadata["checksum"] != "incorrectchecksum"
        assert metadata["checksum"] == shelf.steps[StepURI("snapshot", path)][0].checksum
