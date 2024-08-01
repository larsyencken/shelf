from dataclasses import dataclass
from functools import total_ordering
from typing import Literal

Checksum = str
FileName = str
DatasetName = str
Manifest = dict[FileName, Checksum]
Dag = dict["StepURI", list["StepURI"]]


@total_ordering
@dataclass
class StepURI:
    scheme: Literal["snapshot", "table"]
    path: DatasetName

    @property
    def uri(self):
        return f"{self.scheme}://{self.path}"

    @classmethod
    def parse(cls, uri: str) -> "StepURI":
        scheme, path = uri.split("://")
        if scheme not in ["snapshot", "table"]:
            raise ValueError(f"Unknown scheme: {scheme}")
        return cls(scheme, path)  # type: ignore

    def __str__(self):
        return self.uri

    def __eq__(self, other):
        return self.uri == other.uri

    def __lt__(self, other):
        return self.uri < other.uri

    def __hash__(self):
        return hash(self.uri)
