from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import duckdb
import pandas as pd

app = FastAPI(title="Samarth Data API", version="1.0")

# Enable CORS for frontend (e.g., Streamlit or React)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_PATH = "data/samarth.duckdb"

@app.get("/")
def home():
    return {"message": "✅ Project Samarth Backend running successfully!"}

@app.get("/tables")
def list_tables():
    """List all tables available in DuckDB"""
    con = duckdb.connect(DB_PATH)
    tables = con.execute("SHOW TABLES;").df()
    con.close()
    return {"tables": tables["name"].tolist()}

@app.get("/data")
def get_data(limit: int = Query(10, description="Number of rows to return")):
    """Return sample data from samarth_dataset"""
    con = duckdb.connect(DB_PATH)
    df = con.execute(f"SELECT * FROM samarth_dataset LIMIT {limit};").df()
    con.close()
    return df.to_dict(orient="records")

@app.get("/filter")
def filter_data(year: int = Query(None, description="Filter data by year")):
    """Filter dataset by year"""
    con = duckdb.connect(DB_PATH)
    query = "SELECT * FROM samarth_dataset"
    if year:
        query += f" WHERE year = {year}"
    df = con.execute(query).df()
    con.close()
    return df.to_dict(orient="records")

@app.get("/stats")
def get_stats():
    """Return basic statistics (mean, min, max) for numeric columns"""
    con = duckdb.connect(DB_PATH)
    df = con.execute("SELECT * FROM samarth_dataset").df()
    con.close()
    stats = df.describe(include="all").to_dict()
    return stats
