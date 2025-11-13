import os
import json
import glob
import streamlit as st
import pandas as pd
import duckdb
import requests

# Determine project root and DB path reliably
PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.join(PROJECT_ROOT, "data", "samarth.duckdb")
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")

st.title("📊 Samarth Data Explorer")

def table_exists(con, table_name="samarth_dataset"):
    try:
        con.execute(f"SELECT 1 FROM {table_name} LIMIT 1").fetchall()
        return True
    except Exception:
        return False

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

def build_db_from_raw(con):
    json_files = glob.glob(os.path.join(RAW_DIR, "*.json"))
    if not json_files:
        st.warning("No raw JSON files found in data/raw to build the DB. Create data/raw/*.json or run the ETL first.")
        empty = pd.DataFrame(columns=["year", "year_rank"])
        con.register("tmp_df", empty)
        con.execute("CREATE TABLE IF NOT EXISTS samarth_dataset AS SELECT * FROM tmp_df")
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
            st.warning(f"Failed to parse {jf}: {e}")

    if not dfs:
        st.warning("No records parsed from JSON files. Created empty dataset.")
        empty = pd.DataFrame(columns=["year", "year_rank"])
        con.register("tmp_df", empty)
        con.execute("CREATE TABLE IF NOT EXISTS samarth_dataset AS SELECT * FROM tmp_df")
        return

    full = pd.concat(dfs, ignore_index=True)

    # find a column that represents year (case-insensitive)
    year_col = None
    for c in full.columns:
        if c.lower() == "year":
            year_col = c
            break
    if year_col is None:
        # try common alternatives
        for c in full.columns:
            if "year" in c.lower():
                year_col = c
                break

    if year_col is not None:
        # coerce to numeric year
        full["year"] = pd.to_numeric(full[year_col], errors="coerce").astype("Int64")
        yearly = full.dropna(subset=["year"]).groupby("year").size().reset_index(name="count")
        # Use count as 'year_rank' so the app has something to plot
        yearly = yearly.rename(columns={"count": "year_rank"}).sort_values("year")
        yearly["year_rank"] = yearly["year_rank"].astype(int)
        out_df = yearly[["year", "year_rank"]]
    else:
        # no year found; create empty table with expected columns
        out_df = pd.DataFrame(columns=["year", "year_rank"])

    # write to duckdb
    con.register("tmp_df", out_df)
    con.execute("CREATE TABLE IF NOT EXISTS samarth_dataset AS SELECT * FROM tmp_df")
    st.success(f"Built samarth_dataset with {len(out_df)} rows from {len(json_files)} JSON files.")

# Connect to DuckDB in READ-ONLY mode (Streamlit will be a reader only)
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

try:
    # Always open read-only in the frontend to avoid config conflicts
    con = duckdb.connect(DB_PATH, read_only=True)
except Exception as e:
    st.error(
        "Failed to open DuckDB in read-only mode. Possible causes:\n"
        "- Database file missing (run the ETL/build script)\n"
        "- Another process holds the DB with incompatible settings\n\n"
        f"Error: {e}\n\n"
        "Fix: close other processes using the DB, or run the ETL/build_duckdb.py from a separate process and then restart the app."
    )
    st.stop()

# Do NOT attempt to build or open write connections here.
# If the table is missing, instruct the user to run the ETL script externally.
if not table_exists(con, "samarth_dataset"):
    st.error("samarth_dataset not found in the database. Run: python etl\\build_duckdb.py from project root (in a separate terminal).")
    st.stop()

try:
    df = con.execute("SELECT * FROM samarth_dataset").fetchdf()
except Exception as e:
    st.error(f"Failed to read table 'samarth_dataset' from {DB_PATH}: {e}")
    st.stop()

st.write("Explore the merged data stored in DuckDB")
st.dataframe(df)

# Plot if we have year and year_rank
if "year" in df.columns and "year_rank" in df.columns and not df["year"].isna().all():
    # ensure year is numeric for plotting
    df_plot = df.copy()
    df_plot["year"] = pd.to_numeric(df_plot["year"], errors="coerce")
    df_plot = df_plot.dropna(subset=["year"])
    df_plot = df_plot.sort_values("year").set_index("year")
    st.subheader("📈 Yearly Ranking Chart")
    st.line_chart(df_plot["year_rank"])
else:
    st.warning("⚠️ Columns 'year' and 'year_rank' not found or contain no data in dataset.")

# === QA panel: calls backend /ask (Gemini) ===
st.markdown("---")
st.subheader("🧠 Ask the dataset (QA)")

backend_url = st.text_input("Backend QA URL", value="http://127.0.0.1:8001/ask")
question = st.text_area("Enter your question about the data", height=120)

if st.button("Ask"):
    if not question.strip():
        st.warning("Please enter a question.")
    else:
        with st.spinner("Sending question to backend..."):
            try:
                resp = requests.post(backend_url, json={"query": question}, timeout=30)
                if resp.status_code != 200:
                    st.error(f"Backend returned {resp.status_code}: {resp.text}")
                else:
                    data = resp.json()
                    st.markdown("**Generated SQL:**")
                    st.code(data.get("sql", ""))
                    st.markdown("**Results:**")
                    results = data.get("result", [])
                    if results:
                        st.dataframe(pd.DataFrame(results))
                    else:
                        st.info(data.get("note", "No results returned."))
            except requests.exceptions.RequestException as e:
                st.error(f"Failed to contact backend at {backend_url}: {e}")
                st.info("Start backend: uvicorn backend.qa_engine:app --reload --host 127.0.0.1 --port 8001")
