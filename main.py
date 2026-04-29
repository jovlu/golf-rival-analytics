import subprocess
import sys
from pathlib import Path

# Resolve the project root from this file so the script works from any folder.
project_root = Path(__file__).resolve().parent

# Run the duplicate-removal step first, then validate the deduped file.
scripts_to_run = [
    "scripts/remove_adjacent_duplicate_jsonl.py",
    "scripts/clean_events_jsonl.py",
]

for script_path in scripts_to_run:
    print("running", script_path)

    subprocess.run(
        [sys.executable, script_path],
        check=True,
        cwd=project_root,
    )

print("pipeline finished")
