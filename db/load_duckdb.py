# db/load_duckdb.py
import duckdb
import pandas as pd
from pathlib import Path

NORM_DIR = Path("data/normalized")
DB_PATH = Path("data/samarth.duckdb")

if not NORM_DIR.exists():
    print("❌ data/normalized doesn't exist. Run normalize.py first.")
    raise SystemExit(1)

csv_files = list(NORM_DIR.glob("*.csv"))
if not csv_files:
    print("❌ No CSV files found in data/normalized. Run normalize.py.")
    raise SystemExit(1)

con = duckdb.connect(str(DB_PATH))
print("🔗 Connected to DuckDB:", DB_PATH)

for csv_file in csv_files:
    # create safe table name from file stem
    stem = csv_file.stem
    # normalize names: remove non-alnum and prefix properly
    table_name = stem.lower().replace("-", "_").replace(".", "_")
    # optionally map common names -> readable names (you can expand)
    if "rain" in table_name or "rainfall" in table_name:
        table_name = "rainfall_data"
    elif "crop" in table_name or "production" in table_name:
        table_name = "crop_production_data"
    else:
        table_name = f"table_{table_name}"

    print(f"\n📂 Loading '{csv_file.name}' -> table '{table_name}'")
    df = pd.read_csv(csv_file)
    print(f"   → shape: {df.shape}")

    # Drop old table and create fresh table
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
    print(f"✅ Loaded {len(df)} rows into '{table_name}'")

print("\n📋 Tables now in DB:")
print(con.execute("SHOW TABLES").fetchdf())

# quick preview of each table
tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
for t in tables:
    print(f"\n🔍 Sample from {t}:")
    print(con.execute(f"SELECT * FROM {t} LIMIT 5").fetchdf())

con.close()
print("\n🏁 Done!")
