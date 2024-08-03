import json
from pathlib import Path

SNAPSHOT_SCHEMA_FILE = Path(__file__).parent / "snapshot-v1.schema.json"
TABLE_SCHEMA_FILE = Path(__file__).parent / "table-v1.schema.json"
SHELF_SCHEMA_FILE = Path(__file__).parent / "shelf-v1.schema.json"

SNAPSHOT_SCHEMA = json.loads(SNAPSHOT_SCHEMA_FILE.read_text())
TABLE_SCHEMA = json.loads(TABLE_SCHEMA_FILE.read_text())
SHELF_SCHEMA = json.loads(SHELF_SCHEMA_FILE.read_text())
