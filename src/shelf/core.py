from dataclasses import dataclass, field
from pathlib import Path

import jsonschema
import yaml

from shelf.schemas import SHELF_SCHEMA
from shelf.types import Dag, StepURI
from shelf.utils import print_op

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
        config = yaml.safe_load(self.config_file.read_text())
        jsonschema.validate(config, SHELF_SCHEMA)

        self.version = config["version"]
        self.steps = {
            StepURI.parse(s): [StepURI.parse(d) for d in deps]
            for s, deps in config["steps"].items()
        }

    @staticmethod
    def init(shelf_file: Path = DEFAULT_SHELF_PATH) -> "Shelf":
        if not shelf_file.exists():
            print_op("CREATE", shelf_file)
            shelf_file.write_text(
                yaml.safe_dump(
                    {
                        "version": 1,
                        "data_dir": "data",
                        "steps": {},
                    },
                )
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
        print_op("UPDATE", self.config_file)
        self.config_file.write_text(yaml.safe_dump(config))
