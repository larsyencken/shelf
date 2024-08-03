import os
import shutil

import pytest
from shelf.paths import TABLE_DIR, TABLE_SCRIPT_DIR
from shelf.tables import build_table
from shelf.types import StepURI


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
        mkdir -p $(dirname $1)
        echo "dim_col1,col2" > $1
        echo "1,2" >> $1
        echo "3,4" >> $1
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
        mkdir -p $(dirname $1)
        echo "col1,col2" > $1
        echo "1,2" >> $1
        echo "3,4" >> $1
    """)
    script_path.chmod(0o755)

    # Create TableStep instance
    uri = StepURI.parse("table://dataset/latest.csv")

    with pytest.raises(Exception):
        build_table(uri, [])


# def test_generate_metadata(setup_test_environment):
#     tmp_path = setup_test_environment

#     # Create dummy data frame
#     data_frame = pd.DataFrame({"col1": [1, 3], "col2": [2, 4]})

#     # Create TableStep instance
#     uri = StepURI.parse("table://dataset/version.csv")
#     table_step = TableStep(uri)

#     # Create dummy dependencies
#     dep1 = StepURI.parse("snapshot://dep1")
#     dep1_metadata = {
#         "version": 1,
#         "uri": str(dep1),
#         "checksum": "dffd6021bb2bd5b0af676290809ec3a53191dd81c7f70a4b28688a362182986f",
#         "name": "Dependency 1",
#         "source_name": "Source 1",
#         "source_url": "http://example.com",
#         "date_accessed": "2023-01-01",
#         "access_notes": "Access notes",
#     }
#     dep1_metadata_file = tmp_path / "data/tables/dep1.meta.json"
#     dep1_metadata_file.parent.mkdir(parents=True, exist_ok=True)
#     dep1_metadata_file.write_text(yaml.safe_dump(dep1_metadata))

#     # Generate metadata
#     table_step.generate_metadata(data_frame, [dep1])

#     # Check metadata content
#     metadata = yaml.safe_load(table_step.metadata_file.read_text())
#     assert metadata["version"] == 1
#     assert (
#         metadata["checksum"] == table_step._generate_input_manifest([dep1])[str(dep1)]
#     )
#     assert metadata["input_manifest"] == {str(dep1): dep1_metadata["checksum"]}
#     assert metadata["name"] == dep1_metadata["name"]
#     assert metadata["source_name"] == dep1_metadata["source_name"]
#     assert metadata["source_url"] == dep1_metadata["source_url"]
#     assert metadata["date_accessed"] == dep1_metadata["date_accessed"]
#     assert metadata["access_notes"] == dep1_metadata["access_notes"]
