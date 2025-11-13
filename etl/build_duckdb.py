import os
import json
import glob
import pandas as pd
import duckdb

PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
DB_PATH = os.path.join(PROJECT_ROOT, "data", "samarth.duckdb")

def load_json_records(path):
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    if isinstance(payload, dict):
        if "records" in payload and isinstance(payload["records"], list):
            return payload["records"]
        for v in payload.values():
            if isinstance(v, list):
                return v
        return []
    elif isinstance(payload, list):
        return payload
    return []

def build_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    json_files = glob.glob(os.path.join(RAW_DIR, "*.json"))
    if not json_files:
        print("❌ No JSON files in data/raw. Run the fetcher first.")
        return

    dfs = []
    for jf in json_files:
        try:
            records = load_json_records(jf)
            if not records:
                continue
            df = pd.json_normalize(records)
            df["__source_file"] = os.path.basename(jf)
            dfs.append(df)
        except Exception as e:
            print(f"⚠️ Failed to parse {jf}: {e}")

    if not dfs:
        print("❌ No records parsed from JSON files.")
        return

    full = pd.concat(dfs, ignore_index=True)

    # guess year column
    year_col = None
    for c in full.columns:
        if c.lower() == "year":
            year_col = c
            break
    if year_col is None:
        for c in full.columns:
            if "year" in c.lower():
                year_col = c
                break

    if year_col is not None:
        full["year"] = pd.to_numeric(full[year_col], errors="coerce").astype("Int64")
        yearly = full.dropna(subset=["year"]).groupby("year").size().reset_index(name="count")
        yearly = yearly.rename(columns={"count": "year_rank"}).sort_values("year")
        yearly["year_rank"] = yearly["year_rank"].astype(int)
        out_df = yearly[["year", "year_rank"]]
    else:
        out_df = pd.DataFrame(columns=["year", "year_rank"])

    con = duckdb.connect(DB_PATH)
    con.execute("DROP TABLE IF EXISTS samarth_dataset")
    con.register("tmp_df", out_df)
    con.execute("CREATE TABLE samarth_dataset AS SELECT * FROM tmp_df")
    con.close()
    print(f"✅ Built {DB_PATH} with samarth_dataset ({len(out_df)} rows).")

if __name__ == "__main__":
    build_db()