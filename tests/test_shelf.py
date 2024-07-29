import os
import shutil
from pathlib import Path

import pytest
from shelf import Shelf  # noqa

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


def test_add_file(setup_test_environment):
    tmp_path = setup_test_environment

    # configure test
    path = "test_namespace/test_dataset/2024-07-26"
    data_file = tmp_path / "data" / "test_namespace" / "test_dataset" / "2024-07-26.txt"
    metadata_file = (
        tmp_path / "data" / "test_namespace" / "test_dataset" / "2024-07-26.meta.yaml"
    )
    gitignore_file = tmp_path / ".gitignore"

    # make sure files are clear from previous runs
    data_file.unlink(missing_ok=True)
    metadata_file.unlink(missing_ok=True)
    if gitignore_file.exists():
        gitignore_file.unlink()

    # create dummy file
    new_file = tmp_path / "file1.txt"
    new_file.write_text("Hello, World!")

    # add file to shelf
    os.chdir(tmp_path)
    shelf = Shelf.init()
    shelf.add(str(new_file), path)

    # check for data and metadata
    assert data_file.exists()
    assert metadata_file.exists()

    # check if data path is added to .gitignore
    assert gitignore_file.exists()
    with open(gitignore_file, "r") as f:
        assert f"{path}.txt\n" in f.read()

    # re-fetch it from shelf
    data_file.unlink()
    shelf.get("test_namespace/test_dataset/2024-07-26")
    assert data_file.exists()
    assert data_file.read_text() == "Hello, World!"


def test_shelve_directory(setup_test_environment):
    tmp_path = setup_test_environment

    # configure test
    path = "test_namespace/test_dataset/latest"
    data_path = tmp_path / "data" / path
    metadata_file = (tmp_path / "data" / path).with_suffix(".meta.yaml")
    gitignore_file = tmp_path / ".gitignore"

    # clear from previous runs
    if data_path.exists():
        shutil.rmtree(data_path)
    metadata_file.unlink(missing_ok=True)
    if gitignore_file.exists():
        gitignore_file.unlink()

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
    shelf.add(str(parent), path)

    # check the right local files are created
    assert data_path.is_dir()
    assert metadata_file.exists()
    assert (data_path / "MANIFEST.yaml").exists()

    # check if data path is added to .gitignore
    assert gitignore_file.exists()
    with open(gitignore_file, "r") as f:
        assert f"{path}\n" in f.read()

    # clear the data
    shutil.rmtree(data_path)

    # restore from shelf
    shelf.get(path)

    # check it got restored
    assert data_path.is_dir()
    assert (data_path / "file1.txt").exists()
    assert (data_path / "file2.txt").exists()
    assert (data_path / "MANIFEST.yaml").exists()


def test_add_file_with_arbitrary_depth_namespace(setup_test_environment):
    tmp_path = setup_test_environment

    # configure test
    path = "a/b/c/2024-07-26"
    data_file = tmp_path / "data" / "a" / "b" / "c" / "2024-07-26.txt"
    metadata_file = tmp_path / "data" / "a" / "b" / "c" / "2024-07-26.meta.yaml"

    # make sure files are clear from previous runs
    data_file.unlink(missing_ok=True)
    metadata_file.unlink(missing_ok=True)

    # create dummy file
    new_file = tmp_path / "file1.txt"
    new_file.write_text("Hello, World!")

    # add file to shelf
    os.chdir(tmp_path)
    shelf = Shelf.init()
    shelf.add(str(new_file), path)

    # check for data and metadata
    assert data_file.exists()
    assert metadata_file.exists()

    # re-fetch it from shelf
    data_file.unlink()
    shelf.get("a/b/c/2024-07-26")
    assert data_file.exists()
    assert data_file.read_text() == "Hello, World!"


def test_shelve_directory_with_arbitrary_depth_namespace(setup_test_environment):
    tmp_path = setup_test_environment

    # configure test
    path = "a/b/c/latest"
    data_path = tmp_path / "data" / path
    metadata_file = (tmp_path / "data" / path).with_suffix(".meta.yaml")

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
    os.chdir(tmp_path)
    shelf = Shelf.init()
    dataset_name = shelf.add(str(parent), path)
    assert dataset_name == path

    # check the right local files are created
    assert data_path.is_dir()
    assert metadata_file.exists()
    assert (data_path / "MANIFEST.yaml").exists()

    # clear the data
    shutil.rmtree(data_path)

    # restore from shelf
    shelf.get(path)

    # check it got restored
    assert data_path.is_dir()
    assert (data_path / "file1.txt").exists()
    assert (data_path / "file2.txt").exists()
    assert (data_path / "MANIFEST.yaml").exists()


def test_list_datasets(setup_test_environment):
    tmp_path = setup_test_environment

    # configure test
    path1 = "test_namespace/test_dataset1/2024-07-26"
    path2 = "test_namespace/test_dataset2/2024-07-27"
    new_file1 = tmp_path / "file1.txt"
    new_file2 = tmp_path / "file2.txt"
    new_file1.write_text("Hello, World!")
    new_file2.write_text("Hello, Cosmos!")

    # add files to shelf
    os.chdir(tmp_path)
    shelf = Shelf.init()
    shelf.add(str(new_file1), path1)
    shelf.add(str(new_file2), path2)

    # capture the output of list_datasets
    import sys
    from io import StringIO

    captured_output = StringIO()
    sys.stdout = captured_output
    shelf.list_datasets()
    sys.stdout = sys.__stdout__

    output = captured_output.getvalue().strip().split("\n")
    assert output == [
        "test_namespace/test_dataset1/2024-07-26",
        "test_namespace/test_dataset2/2024-07-27",
    ]


def test_list_datasets_with_regex(setup_test_environment):
    tmp_path = setup_test_environment

    # configure test
    path1 = "test_namespace/test_dataset1/2024-07-26"
    path2 = "test_namespace/test_dataset2/2024-07-27"
    new_file1 = tmp_path / "file1.txt"
    new_file2 = tmp_path / "file2.txt"
    new_file1.write_text("Hello, World!")
    new_file2.write_text("Hello, Cosmos!")

    # add files to shelf
    os.chdir(tmp_path)
    shelf = Shelf.init()
    shelf.add(str(new_file1), path1)
    shelf.add(str(new_file2), path2)

    # capture the output of list_datasets with regex
    import sys
    from io import StringIO

    captured_output = StringIO()
    sys.stdout = captured_output
    shelf.list_datasets("test_dataset1")
    sys.stdout = sys.__stdout__

    output = captured_output.getvalue().strip().split("\n")
    assert output == ["test_namespace/test_dataset1/2024-07-26"]


def test_get_only_out_of_date_datasets(setup_test_environment):
    tmp_path = setup_test_environment

    # configure test
    path1 = "test_namespace/test_dataset1/2024-07-26"
    path2 = "test_namespace/test_dataset2/2024-07-27"
    new_file1 = tmp_path / "file1.txt"
    new_file2 = tmp_path / "file2.txt"
    new_file1.write_text("Hello, World!")
    new_file2.write_text("Hello, Cosmos!")

    # add files to shelf
    os.chdir(tmp_path)
    shelf = Shelf.init()
    shelf.add(str(new_file1), path1)
    shelf.add(str(new_file2), path2)

    # modify one of the files to make it out of date
    data_file1 = tmp_path / "data" / "test_namespace" / "test_dataset1" / "2024-07-26.txt"
    data_file1.write_text("Modified content")

    # restore datasets
    shelf.get()

    # check that only the out-of-date dataset was fetched
    assert data_file1.read_text() == "Hello, World!"
    data_file2 = tmp_path / "data" / "test_namespace" / "test_dataset2" / "2024-07-27.txt"
    assert data_file2.read_text() == "Hello, Cosmos!"


def test_get_with_force_option(setup_test_environment):
    tmp_path = setup_test_environment

    # configure test
    path1 = "test_namespace/test_dataset1/2024-07-26"
    path2 = "test_namespace/test_dataset2/2024-07-27"
    new_file1 = tmp_path / "file1.txt"
    new_file2 = tmp_path / "file2.txt"
    new_file1.write_text("Hello, World!")
    new_file2.write_text("Hello, Cosmos!")

    # add files to shelf
    os.chdir(tmp_path)
    shelf = Shelf.init()
    shelf.add(str(new_file1), path1)
    shelf.add(str(new_file2), path2)

    # modify one of the files to make it out of date
    data_file1 = tmp_path / "data" / "test_namespace" / "test_dataset1" / "2024-07-26.txt"
    data_file1.write_text("Modified content")

    # restore datasets with --force option
    shelf.get(force=True)

    # check that both datasets were fetched
    assert data_file1.read_text() == "Hello, World!"
    data_file2 = tmp_path / "data" / "test_namespace" / "test_dataset2" / "2024-07-27.txt"
    assert data_file2.read_text() == "Hello, Cosmos!"
