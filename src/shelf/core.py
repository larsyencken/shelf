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
        with self.config_file.open("r") as istream:
            config = yaml.safe_load(istream)

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
            with shelf_file.open("w") as ostream:
                yaml.dump(
                    {
                        "version": 1,
                        "data_dir": "data",
                        "steps": {},
                    },
                    ostream,
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
        with self.config_file.open("w") as ostream:
            yaml.dump(
                config,
                ostream,
            )
