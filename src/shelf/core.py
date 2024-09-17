from dataclasses import dataclass, field
from pathlib import Path

import jsonschema

from shelf.schemas import SHELF_SCHEMA
from shelf.types import Dag, StepURI
from shelf.utils import load_yaml, save_yaml

DEFAULT_SHELF_PATH = Path("shelf.yaml")


@dataclass
class Shelf:
    config_file: Path
    steps: Dag = field(default_factory=dict)
    version: int = 1

    def __init__(self, config_file: Path = DEFAULT_SHELF_PATH):
        "Load an existing shelf.yaml file from disk."
        if not config_file.exists():
            raise FileNotFoundError("shelf.yaml not found")

        self.config_file = config_file
        self.refresh()

    def refresh(self) -> None:
        config = load_yaml(self.config_file)
        jsonschema.validate(config, SHELF_SCHEMA)

        self.version = config["version"]
        self.steps = {
            StepURI.parse(s): [StepURI.parse(d) for d in deps]
            for s, deps in config["steps"].items()
        }

    @staticmethod
    def init(shelf_file: Path = DEFAULT_SHELF_PATH) -> "Shelf":
        if not shelf_file.exists():
            save_yaml(
                {
                    "version": 1,
                    "data_dir": "data",
                    "steps": {},
                },
                shelf_file,
            )
        else:
            print(f"{shelf_file} already exists")

        return Shelf()

    def save(self) -> None:
        config = {
            "version": self.version,
            "steps": {
                str(k): [str(v) for v in vs] for k, vs in sorted(self.steps.items())
            },
        }
        jsonschema.validate(config, SHELF_SCHEMA)
        save_yaml(config, self.config_file)

    def new_table(self, table_path: str, dependencies: list[str]) -> None:
        table_uri = StepURI("table", table_path)
        if table_uri in self.steps:
            raise ValueError(f"Table already exists in shelf: {table_uri}")

        self.steps[table_uri] = [StepURI.parse(dep) for dep in dependencies]
        self.save()

    def get_latest_version(self, step: StepURI) -> StepURI:
        assert step.path.endswith("/latest")
        prefix = step.path.rsplit("/", 1)[0]
        versions = [
            s
            for s in self.steps
            if s.scheme == step.scheme and s.path.startswith(prefix)
        ]
        return max(versions)
