import os
import re
import duckdb
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Any, Dict, List

# Google GenAI client
from google import genai
from google.genai import types

app = FastAPI(title="Project Samarth QA Engine")

PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.join(PROJECT_ROOT, "data", "samarth.duckdb")

if not os.path.exists(DB_PATH):
    raise RuntimeError(f"DuckDB file not found: {DB_PATH}. Run etl/build_duckdb.py after fetching data.")

# single shared read-only DuckDB connection for the backend
try:
    con = duckdb.connect(DB_PATH, read_only=True)
except Exception as e:
    raise RuntimeError(f"Failed to open DuckDB in backend: {e}")

GEMINI_API_KEY = "AIzaSyDR4MC6SNULLT6udKpyyW1MnXexRCPIV24"
if not GEMINI_API_KEY:
    raise RuntimeError("GEMINI_API_KEY not set in environment. Export it before starting the API.")

# init client (reuse across requests)
genai_client = genai.Client(api_key=GEMINI_API_KEY)

class Question(BaseModel):
    query: str

def local_sql_fallback(user_query: str) -> str | None:
    q = user_query.lower()
    if "count" in q and "year" in q:
        return "SELECT year, COUNT(*) AS year_rank FROM samarth_dataset GROUP BY year ORDER BY year"
    if ("top" in q or "highest" in q or "max" in q) and "year_rank" in q:
        return "SELECT * FROM samarth_dataset ORDER BY year_rank DESC LIMIT 10"
    if "list" in q or "show all" in q or "select *" in q:
        return "SELECT * FROM samarth_dataset LIMIT 200"
    m = re.search(r"by (\w+)", q)
    if m:
        col = m.group(1)
        return f"SELECT {col}, COUNT(*) AS cnt FROM samarth_dataset GROUP BY {col} ORDER BY cnt DESC LIMIT 200"
    return None

def extract_sql_from_text(text: str) -> str | None:
    if not text:
        return None
    # Extract code block if present
    m = re.search(r"```(?:sql)?\s*(.*?)```", text, re.S | re.I)
    if m:
        text = m.group(1)
    # Find first SELECT or WITH (common SQL starts)
    m2 = re.search(r"\b(select|with)\b", text, re.I)
    if m2:
        sql = text[m2.start():].strip()
        # remove surrounding backticks or quotes and trailing semicolons/newlines
        sql = sql.strip("`\"' \n\r\t;")
        return sql
    # as a last resort, if the text looks like a single-line SQL (contains SELECT)
    if "select" in text.lower():
        # return from first select
        idx = text.lower().find("select")
        sql = text[idx:].strip().strip("`\"' \n\r\t;")
        return sql
    return None

@app.post("/ask")
def ask_question(question: Question):
    user_query = (question.query or "").strip()
    if not user_query:
        raise HTTPException(status_code=400, detail="Empty query")

    prompt = f"""
You are an intelligent data assistant for agriculture datasets.
The main DuckDB table is called 'samarth_dataset' with columns like 'year' and 'year_rank'.
User question: {user_query}

Generate ONE valid SQL query (DuckDB compatible) that answers the question. Only return the SQL query, no explanation.
"""

    fallback_note = None
    try:
        # attempt Gemini streaming generation
        contents = [
            types.Content(
                role="user",
                parts=[types.Part.from_text(text=prompt)],
            )
        ]
        gen_config = types.GenerateContentConfig()
        collected: List[str] = []
        for chunk in genai_client.models.generate_content_stream(
            model="gemini-2.5-flash",
            contents=contents,
            config=gen_config,
        ):
            txt = getattr(chunk, "text", None)
            if isinstance(txt, str) and txt:
                collected.append(txt)
            else:
                delta = getattr(chunk, "delta", None)
                if isinstance(delta, str) and delta:
                    collected.append(delta)

        raw_output = "".join(collected).strip()
        sql_candidate = extract_sql_from_text(raw_output) or (raw_output if raw_output else None)

        # If model didn't produce usable SQL, try local fallback
        if not sql_candidate:
            sql_candidate = local_sql_fallback(user_query)
            if sql_candidate:
                fallback_note = "Used local SQL fallback because Gemini returned no usable SQL."
            else:
                raise HTTPException(
                    status_code=400,
                    detail=f"Gemini returned no usable SQL. Last model output: {raw_output[:1000]!r}"
                )

        # Normalize and validate SQL candidate
        sql_candidate = sql_candidate.strip().rstrip(";")
        if not re.match(r"^(select|with)\b", sql_candidate, re.I):
            # try local fallback before failing
            fallback = local_sql_fallback(user_query)
            if fallback:
                sql_candidate = fallback
                fallback_note = (fallback_note or "") + " Replaced non-SELECT model output with local fallback."
            else:
                raise HTTPException(
                    status_code=400,
                    detail=f"Generated SQL is not a SELECT/WITH query. Generated output (truncated): {sql_candidate[:500]!r}"
                )

        # execute safely
        try:
            df = con.execute(sql_candidate).fetchdf()
        except Exception as e:
            # include SQL in error for debugging
            raise HTTPException(status_code=400, detail=f"Failed to execute SQL: {e}; SQL: {sql_candidate}")

        result = df.to_dict(orient="records") if not df.empty else []
        resp = {"query": user_query, "sql": sql_candidate, "result": result}
        if fallback_note:
            resp["note"] = fallback_note
        if not result:
            resp.setdefault("note", "No rows returned.")
        return resp

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

