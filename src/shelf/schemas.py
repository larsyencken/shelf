import json
from pathlib import Path

METADATA_SCHEMA_FILE = Path(__file__).parent / "metadata-v1.schema.json"
SHELF_SCHEMA_FILE = Path(__file__).parent / "shelf-v1.schema.json"

METADATA_SCHEMA = json.loads(METADATA_SCHEMA_FILE.read_text())
SHELF_SCHEMA = json.loads(SHELF_SCHEMA_FILE.read_text())
