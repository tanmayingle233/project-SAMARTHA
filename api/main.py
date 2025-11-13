from fastapi import FastAPI
import duckdb
import pandas as pd

app = FastAPI(
    title="Project Samarth API",
    description="An intelligent Q&A and data access API for agricultural and climate datasets from data.gov.in",
    version="1.0"
)

# ✅ Root route
@app.get("/")
def home():
    return {"message": "Welcome to Project Samarth Q&A API!"}


# ✅ Route to get all data from DuckDB
@app.get("/data")
def get_data():
    try:
        con = duckdb.connect("data/samarth.duckdb")
        df = con.execute("SELECT * FROM agriculture_data").fetchdf()
        return df.to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}


# ✅ Route for summary statistics (optional but useful)
@app.get("/stats")
def get_summary_stats():
    try:
        con = duckdb.connect("data/samarth.duckdb")
        df = con.execute("SELECT * FROM agriculture_data").fetchdf()

        stats = {
            "total_records": len(df),
            "year_range": f"{df['year'].min()} - {df['year'].max()}",
            "avg_rank": round(df['year_rank'].mean(), 2),
            "best_year": int(df.loc[df['year_rank'].idxmin(), 'year']),
            "worst_year": int(df.loc[df['year_rank'].idxmax(), 'year']),
        }

        return stats

    except Exception as e:
        return {"error": str(e)}


# ✅ Route to get data for a specific year
@app.get("/year/{year}")
def get_data_by_year(year: int):
    try:
        con = duckdb.connect("data/samarth.duckdb")
        query = f"SELECT * FROM agriculture_data WHERE year = {year}"
        df = con.execute(query).fetchdf()
        if df.empty:
            return {"message": f"No data found for year {year}"}
        return df.to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}
