import os
import random
import shutil
from typing import Any, Optional

import pytest
import yaml
from shelf.paths import SNAPSHOT_DIR, TABLE_DIR, TABLE_SCRIPT_DIR
from shelf.tables import _metadata_path, build_table
from shelf.types import StepURI
from shelf.utils import checksum_file


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


def test_generate_without_deps(setup_test_environment):
    # Create dummy dependencies
    expected_content = "dim_col1,col2\n1,2\n3,4\n"

    # Create dummy script
    uri = StepURI.parse("table://dataset/latest.csv")
    script_path = TABLE_SCRIPT_DIR / "dataset/latest"
    script_path.parent.mkdir(parents=True, exist_ok=True)
    script_path.write_text("""#!/bin/bash
        dest="${!#}"
        mkdir -p $(dirname $dest)
        echo "dim_col1,col2" > $dest
        echo "1,2" >> $dest
        echo "3,4" >> $dest
    """)
    script_path.chmod(0o755)

    # Create TableStep instance
    uri = StepURI.parse("table://dataset/latest.csv")

    build_table(uri, [])

    # Check data frame content
    dest_file = TABLE_DIR / "dataset/latest.csv"
    assert dest_file.exists()
    assert dest_file.read_text() == expected_content


def test_generate_without_dimension_col(setup_test_environment):
    # Create dummy script
    uri = StepURI.parse("table://dataset/latest.csv")
    script_path = TABLE_SCRIPT_DIR / "dataset/latest"
    script_path.parent.mkdir(parents=True, exist_ok=True)
    script_path.write_text("""#!/bin/bash
        dest="${!#}"
        mkdir -p $(dirname $dest)
        echo "col1,col2" > $dest
        echo "1,2" >> $dest
        echo "3,4" >> $dest
    """)
    script_path.chmod(0o755)

    uri = StepURI.parse("table://dataset/latest.csv")

    with pytest.raises(Exception):
        build_table(uri, [])


def test_generate_with_deps(setup_test_environment):
    dep1 = add_mock_snapshot()
    dep2 = add_mock_snapshot()
    deps = [dep1, dep2]

    # Create dummy script
    uri = StepURI.parse("table://dataset/latest.csv")
    script_path = TABLE_SCRIPT_DIR / "dataset/latest"
    script_path.parent.mkdir(parents=True, exist_ok=True)
    script_path.write_text("""#!/bin/bash
        dest="${!#}"
        mkdir -p $(dirname $dest)
        echo "dim_col1,col2" > $dest
        echo "1,2" >> $dest
        echo "3,4" >> $dest
    """)
    script_path.chmod(0o755)

    build_table(uri, deps)


def test_generate_with_single_dep(setup_test_environment):
    expected_metadata = {
        "name": "Huzzah",
        "source_name": "Huzzah",
        "source_url": "https://a.com/b/c",
    }
    dep = add_mock_snapshot(expected_metadata)
    deps = [dep]

    # Create dummy script
    uri = StepURI.parse("table://dataset/latest.csv")
    script_path = TABLE_SCRIPT_DIR / "dataset/latest"
    script_path.parent.mkdir(parents=True, exist_ok=True)
    script_path.write_text("""#!/bin/bash
        dest="${!#}"
        mkdir -p $(dirname $dest)
        echo "dim_col1,col2" > $dest
        echo "1,2" >> $dest
        echo "3,4" >> $dest
    """)
    script_path.chmod(0o755)

    build_table(uri, deps)

    metadata = yaml.safe_load(_metadata_path(uri).read_text())
    for key, value in expected_metadata.items():
        assert metadata[key] == value


def add_mock_snapshot(metadata: Optional[dict[str, Any]] = None) -> StepURI:
    # choose the uri
    uri = StepURI("snapshot", random_path())

    # make the matching data file
    fill = "\n".join(random_string() for _ in range(5))
    dest_path = (SNAPSHOT_DIR / uri.path).with_suffix(".txt")
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    dest_path.write_text(fill)

    # make the matching metadata file
    metadata_path = dest_path.with_suffix(".meta.yaml")
    metadata_path.write_text(
        yaml.safe_dump(
            {
                "uri": str(uri),
                "version": 1,
                "checksum": checksum_file(dest_path),
                "extension": ".txt",
                "snapshot_type": "file",
                **(metadata or {}),
            }
        )
    )

    return uri


def random_string() -> str:
    return "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=80))


def random_path() -> str:
    # return a path between 1 and 5 segments deep
    n_segments = random.randint(1, 5)
    parts = []
    for _ in range(n_segments):
        parts.append(random.choice(["a", "b", "c", "d", "e"]))

    if random.random() < 0.5:
        parts.append("latest")
    else:
        parts.append(random_date())

    return "/".join(parts)


def random_date() -> str:
    return f"{random.randint(2000, 2021)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"
