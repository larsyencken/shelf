import os
import subprocess
from pathlib import Path
from typing import Optional, Union

import pandas as pd
import jsonschema
import yaml

from shelf.paths import BASE_DIR, DATA_DIR
from shelf.schemas import METADATA_SCHEMA
from shelf.types import Checksum, DatasetName, StepURI
from shelf.utils import checksum_file, checksum_manifest, print_op


class TableStep:
    def __init__(self, uri: StepURI):
        self.uri = uri
        self.dataset_path, self.version, self.extension = self._parse_uri(uri)
        self.data_file = DATA_DIR / "tables" / self.dataset_path / f"{self.version}.{self.extension}"
        self.metadata_file = DATA_DIR / "tables" / self.dataset_path / f"{self.version}.meta.json"

    def _parse_uri(self, uri: StepURI):
        parts = uri.path.split("/")
        dataset_path = "/".join(parts[:-1])
        version, extension = parts[-1].split(".")
        return dataset_path, version, extension

    # FIXME we don't care about a data frame here
    def generate_data_frame(self, dependencies: list[Path]) -> pd.DataFrame:
        script_path = self._find_executable_script()
        output_file = self.data_file

        command = [str(script_path)] + [str(dep) for dep in dependencies] + [str(output_file)]
        subprocess.run(command, check=True)

        if not output_file.exists():
            raise FileNotFoundError(f"Output file {output_file} not found after execution")

        return pd.read_csv(output_file)

    def _find_executable_script(self) -> Path:
        script_path = Path(f"steps/tables/{self.dataset_path}/{self.version}")
        if script_path.exists() and os.access(script_path, os.X_OK):
            return script_path

        script_path = Path(f"steps/tables/{self.dataset_path}")
        if script_path.exists() and os.access(script_path, os.X_OK):
            return script_path

        raise FileNotFoundError(f"No executable script found for table step {self.uri}")

    def generate_metadata(self, data_frame: pd.DataFrame, dependencies: list[StepURI]) -> None:
        metadata = {
            "version": 1,
            "checksum": checksum_file(self.data_file),
            "input_manifest": self._generate_input_manifest(dependencies),
        }

        if len(dependencies) == 1:
            dep_metadata = self._load_metadata(dependencies[0])
            for field in ["name", "source_name", "source_url", "date_accessed", "access_notes"]:
                if field in dep_metadata:
                    metadata[field] = dep_metadata[field]

        jsonschema.validate(metadata, METADATA_SCHEMA)
        self.metadata_file.write_text(yaml.safe_dump(metadata))

    def _generate_input_manifest(self, dependencies: list[StepURI]) -> dict[str, Checksum]:
        manifest = {}
        for dep in dependencies:
            dep_metadata = self._load_metadata(dep)
            manifest[str(dep)] = dep_metadata["checksum"]
        return manifest

    def _load_metadata(self, uri: StepURI) -> dict:
        metadata_file = (DATA_DIR / "tables" / uri.path).with_suffix(".meta.json")
        return yaml.safe_load(metadata_file.read_text())
