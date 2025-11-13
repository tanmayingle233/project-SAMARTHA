import duckdb
import os
import pandas as pd

# ✅ Path to your DuckDB file
DB_PATH = os.path.join("data", "samarth.duckdb")

print(f"🔗 Connecting to DuckDB: {DB_PATH}")
con = duckdb.connect(DB_PATH)

# ✅ Step 1: Get all table names
tables_df = con.execute("SHOW TABLES").fetchdf()
table_names = tables_df['name'].tolist()

print("\n📋 Tables found in database:")
for name in table_names:
    print("   →", name)

# ✅ Step 2: Select only normalized tables (excluding base tables like agriculture_data)
normalized_tables = [t for t in table_names if t.startswith("table_") and t.endswith("_normalized")]

if not normalized_tables:
    print("\n⚠️ No normalized tables found to merge.")
    con.close()
    exit()

print("\n🧩 Normalized tables to merge:")
for t in normalized_tables:
    print("   →", t)

# ✅ Step 3: Read and merge all normalized tables
merged_df = None
for t in normalized_tables:
    df = con.execute(f"SELECT * FROM {t}").fetchdf()
    if merged_df is None:
        merged_df = df
    else:
        # Merge on common columns (auto-detect)
        common_cols = list(set(merged_df.columns) & set(df.columns))
        merged_df = pd.merge(merged_df, df, on=common_cols, how="outer")

# ✅ Step 4: Store merged dataset into DuckDB
con.execute("DROP TABLE IF EXISTS samarth_dataset")
con.register("merged_df", merged_df)
con.execute("CREATE TABLE samarth_dataset AS SELECT * FROM merged_df")

print(f"\n✅ Merged {len(normalized_tables)} tables into 'samarth_dataset'")
print(f"🧮 Total records in final dataset: {len(merged_df)}")

# ✅ Step 5: Show preview
print("\n🔍 Sample from merged dataset:")
print(merged_df.head())

# ✅ Step 6: Cleanup
con.close()
print("\n🏁 Done! All datasets successfully merged into 'samarth_dataset'.")
