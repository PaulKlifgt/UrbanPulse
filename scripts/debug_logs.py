import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from scripts.patch_from_logs import parse_logs, LOGS
import json

updates = parse_logs(LOGS)
from scripts.patch_from_logs import apply_updates
apply_updates(updates)
