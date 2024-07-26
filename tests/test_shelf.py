import os
import shutil
from pathlib import Path

import pytest
from shelf import add, get  # noqa

BASE = Path(__file__).parent.parent
DATA_DIR = BASE / "data"
METADATA_DIR = BASE / "metadata"


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

    yield test_dir

    # Cleanup
    shutil.rmtree(test_dir)


def test_add_file(setup_test_environment):
    tmp_path = setup_test_environment

    # configure test
    path = "test_namespace/test_dataset/2024-07-26"
    data_file = DATA_DIR / "test_namespace" / "test_dataset" / "2024-07-26.txt"
    metadata_file = METADATA_DIR / "test_namespace" / "test_dataset" / "2024-07-26.yaml"

    # make sure files are clear from previous runs
    data_file.unlink(missing_ok=True)
    metadata_file.unlink(missing_ok=True)

    # create dummy file
    new_file = tmp_path / "file1.txt"
    new_file.write_text("Hello, World!")

    # add file to shelf
    add(str(new_file), path)

    # check for data and metadata
    assert data_file.exists()
    assert metadata_file.exists()

    # re-fectch it from shelf
    data_file.unlink()
    get("test_namespace/test_dataset/2024-07-26")
    assert data_file.exists()
    assert data_file.read_text() == "Hello, World!"


def test_shelve_directory(setup_test_environment):
    tmp_path = setup_test_environment

    # configure test
    path = "test_namespace/test_dataset/latest"
    data_path = DATA_DIR / path
    metadata_file = (METADATA_DIR / path).with_suffix(".yaml")

    # clear from previous runs
    if data_path.exists():
        shutil.rmtree(data_path)
    metadata_file.unlink(missing_ok=True)

    # create dummy data
    parent = tmp_path / "example"
    if parent.exists():
        shutil.rmtree(parent)
    parent.mkdir()
    (parent / "file1.txt").write_text("Hello, World!")
    (parent / "file2.txt").write_text("Hello, Cosmos!")

    # add to shelf
    add(str(parent), path)

    # check the right local files are created
    assert data_path.is_dir()
    assert metadata_file.exists()
    assert (data_path / "MANIFEST.yaml").exists()

    # clear the data
    shutil.rmtree(data_path)

    # restore from shelf
    get(path)

    # check it got restored
    assert data_path.is_dir()
    assert (data_path / "file1.txt").exists()
    assert (data_path / "file2.txt").exists()
    assert (data_path / "MANIFEST.yaml").exists()


def test_add_file_with_arbitrary_depth_namespace(setup_test_environment):
    tmp_path = setup_test_environment

    # configure test
    path = "a/b/c/2024-07-26"
    data_file = DATA_DIR / "a" / "b" / "c" / "2024-07-26.txt"
    metadata_file = METADATA_DIR / "a" / "b" / "c" / "2024-07-26.yaml"

    # make sure files are clear from previous runs
    data_file.unlink(missing_ok=True)
    metadata_file.unlink(missing_ok=True)

    # create dummy file
    new_file = tmp_path / "file1.txt"
    new_file.write_text("Hello, World!")

    # add file to shelf
    add(str(new_file), path)

    # check for data and metadata
    assert data_file.exists()
    assert metadata_file.exists()

    # re-fetch it from shelf
    data_file.unlink()
    get("a/b/c/2024-07-26")
    assert data_file.exists()
    assert data_file.read_text() == "Hello, World!"


def test_shelve_directory_with_arbitrary_depth_namespace(setup_test_environment):
    tmp_path = setup_test_environment

    # configure test
    path = "a/b/c/latest"
    data_path = DATA_DIR / path
    metadata_file = (METADATA_DIR / path).with_suffix(".yaml")

    # clear from previous runs
    if data_path.exists():
        shutil.rmtree(data_path)
    metadata_file.unlink(missing_ok=True)

    # create dummy data
    parent = tmp_path / "example"
    if parent.exists():
        shutil.rmtree(parent)
    parent.mkdir()
    (parent / "file1.txt").write_text("Hello, World!")
    (parent / "file2.txt").write_text("Hello, Cosmos!")

    # add to shelf
    dataset_name = add(str(parent), path)
    assert dataset_name == path

    # check the right local files are created
    assert data_path.is_dir()
    assert metadata_file.exists()
    assert (data_path / "MANIFEST.yaml").exists()

    # clear the data
    shutil.rmtree(data_path)

    # restore from shelf
    get(path)

    # check it got restored
    assert data_path.is_dir()
    assert (data_path / "file1.txt").exists()
    assert (data_path / "file2.txt").exists()
    assert (data_path / "MANIFEST.yaml").exists()
