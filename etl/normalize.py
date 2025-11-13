import json
import pandas as pd
from pathlib import Path

# Input and output folders
RAW_DIR = Path("data/raw")
NORM_DIR = Path("data/normalized")
NORM_DIR.mkdir(parents=True, exist_ok=True)

# Load the first JSON file in raw data
json_files = list(RAW_DIR.glob("*.json"))
if not json_files:
    print("❌ No raw JSON files found. Run ckan_fetcher.py first.")
    exit()

raw_file = json_files[0]
print(f"🔹 Normalizing data from: {raw_file}")

# Load JSON content

with open(raw_file, "r", encoding="utf-8") as f:
    data = json.load(f)

# Extract records
records = data.get("records", [])
if not records:
    print("❌ No records found in the JSON file.")
    exit()

# Convert to DataFrame
df = pd.DataFrame(records)

# Basic cleaning: remove empty columns and trim whitespace
df = df.dropna(axis=1, how="all")
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# Save normalized CSV
csv_path = NORM_DIR / f"{raw_file.stem}_normalized.csv"
df.to_csv(csv_path, index=False, encoding="utf-8")
print(f"✅ Normalized data saved to {csv_path}")
print(f"🧮 Total records processed: {len(df)}")
