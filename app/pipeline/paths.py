from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
EVENTS_FILE = PROJECT_ROOT / "events.jsonl"
DEDUPED_EVENTS_FILE = PROJECT_ROOT / "events.deduped.jsonl"
CLEANED_EVENTS_FILE = PROJECT_ROOT / "events.cleaned.jsonl"
MAPS_FILE = PROJECT_ROOT / "maps.jsonl"
