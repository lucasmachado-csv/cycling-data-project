"""
Lightweight FastAPI service that turns natural language into SQL for DuckDB
with guardrails and read-only execution.

Env vars:
- DUCKDB_PATH: path to duckdb file (default: /Users/lucas/Documents/BAM/cycling.duckdb)
- OPENAI_API_KEY: required for GPT NL->SQL
- CHATBOT_OPENAI_MODEL: model name (default: gpt-4.1-mini)
"""
import os
import re
import json
from typing import Dict, List, Any, Optional, Tuple

import duckdb
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from openai import OpenAI

# Load .env if python-dotenv is available (allows OPENAI_API_KEY, etc. from .env)
try:  # pragma: no cover - optional convenience
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None

if load_dotenv:  # pragma: no cover
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

DB_PATH = os.getenv("DUCKDB_PATH", "/Users/lucas/Documents/BAM/cycling.duckdb")
OPENAI_MODEL = os.getenv("CHATBOT_OPENAI_MODEL", "gpt-4.1-mini")

ALLOWED_TABLES = [
    "london_bike_data",
    "nyc_biking_data",
    "joint_bike_data",
]


class AskRequest(BaseModel):
    question: str
    max_rows: int = 50


class AskResponse(BaseModel):
    answer: str
    sql: str
    data_preview: List[Dict[str, Any]]
    columns: List[str]


def get_schema_snapshot() -> Dict[str, Dict[str, str]]:
    """Return {table: {column: type}} for allowed tables."""
    con = duckdb.connect(DB_PATH, read_only=True)
    snapshot: Dict[str, Dict[str, str]] = {}
    for table in ALLOWED_TABLES:
        if not con.execute(
            "select count(*) from information_schema.tables where table_name=?",
            [table],
        ).fetchone()[0]:
            continue
        rows = con.execute(
            """
            select column_name, data_type
            from information_schema.columns
            where table_name=?
            order by ordinal_position
            """,
            [table],
        ).fetchall()
        snapshot[table] = {c: t for c, t in rows}
    con.close()
    return snapshot


SCHEMA = get_schema_snapshot()


def build_system_prompt() -> str:
    lines = [
        "You are an assistant that writes one safe, efficient SQL query for DuckDB.",
        "Rules:",
        "- Only use tables and columns listed below.",
        "- Prefer aggregates; always include LIMIT on raw row queries.",
        "- Never run DDL/DML; only SELECT.",
        "- Return just SQL, no prose.",
        "",
        "Schema:",
    ]
    for table, cols in SCHEMA.items():
        col_list = ", ".join(cols.keys())
        lines.append(f"- {table}({col_list})")
    lines.append("")
    lines.append("Examples:")
    lines.append(
        "1) Total rides in London 2020\n"
        "SELECT count(*) AS ride_count FROM london_bike_data "
        "WHERE start_date >= '2020-01-01' AND start_date < '2021-01-01';"
    )
    lines.append(
        "2) Median ride duration (minutes) NYC July 2020\n"
        "SELECT median(date_diff('minute', start_time, end_time)) AS median_minutes "
        "FROM nyc_biking_data "
        "WHERE start_time >= '2020-07-01' AND start_time < '2020-08-01';"
    )
    lines.append(
        "3) Top 5 start stations London last 30 days\n"
        "SELECT start_station_name, count(*) AS rides "
        "FROM london_bike_data "
        "WHERE start_date >= now() - INTERVAL 30 DAY "
        "GROUP BY start_station_name "
        "ORDER BY rides DESC LIMIT 5;"
    )
    lines.append(
        "4) Avg duration by hour NYC 2021\n"
        "SELECT date_part('hour', start_time) AS hour, "
        "avg(date_diff('minute', start_time, end_time)) AS avg_minutes "
        "FROM nyc_biking_data "
        "WHERE start_time >= '2021-01-01' AND start_time < '2022-01-01' "
        "GROUP BY hour ORDER BY hour;"
    )
    return "\n".join(lines)


SYSTEM_PROMPT = build_system_prompt()


def extract_sql(text: str) -> str:
    fence = re.search(r"```sql\\s*(.*?)```", text, re.IGNORECASE | re.DOTALL)
    if fence:
        return fence.group(1).strip().rstrip(";")
    return text.strip().rstrip(";")


def is_aggregate(sql_lc: str) -> bool:
    agg_keywords = ["group by", "count(", "avg(", "sum(", "median(", "percentile", "histogram("]
    return any(k in sql_lc for k in agg_keywords)


def validate_sql(sql: str, schema: Dict[str, Dict[str, str]]) -> Tuple[bool, str]:
    sql_lc = sql.lower()
    if ";" in sql_lc:
        return False, "Multiple statements are not allowed."
    banned = ["insert", "update", "delete", "create", "drop", "alter", "attach", "pragma", "copy", "truncate"]
    if any(re.search(rf"\\b{kw}\\b", sql_lc) for kw in banned):
        return False, "Only read-only SELECT is allowed."
    if not sql_lc.startswith("select"):
        return False, "Query must start with SELECT."
    # Extract tables after FROM / JOIN
    tables = re.findall(r"\\bfrom\\s+([\\w\\.]+)", sql_lc) + re.findall(r"\\bjoin\\s+([\\w\\.]+)", sql_lc)
    tables = [t.split(".")[-1] for t in tables]
    for t in tables:
        if t not in schema:
            return False, f"Table {t} is not allowed."
    # Require LIMIT for non-aggregate queries
    if not is_aggregate(sql_lc) and "limit" not in sql_lc:
        return False, "Please include a LIMIT for non-aggregate queries."
    return True, ""


client = OpenAI()
app = FastAPI(title="DuckDB NL→SQL Chatbot", version="0.1.0")


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok", "tables": ",".join(SCHEMA.keys())}


@app.get("/", response_class=HTMLResponse)
def root() -> str:
    """Minimal front-end to interact with the chatbot."""
    return """
    <!doctype html>
    <html>
    <head>
      <meta charset="utf-8">
      <title>DuckDB NL→SQL Chatbot</title>
      <style>
        :root {
          color-scheme: light;
          --bg: #f5f7fb;
          --card: #ffffff;
          --border: #d9deea;
          --text: #1f2937;
          --muted: #6b7280;
          --accent: #2563eb;
          --accent-soft: #e0e7ff;
          --success: #16a34a;
          --error: #dc2626;
          --mono: SFMono-Regular, Consolas, "Liberation Mono", Menlo, monospace;
        }
        body {
          margin: 0;
          padding: 0;
          font-family: "Inter", "Segoe UI", system-ui, -apple-system, sans-serif;
          background: var(--bg);
          color: var(--text);
        }
        .shell {
          max-width: 1080px;
          margin: 0 auto;
          padding: 32px 20px 48px;
        }
        h1 {
          margin: 0 0 8px;
          font-weight: 700;
          letter-spacing: -0.01em;
        }
        .sub {
          margin: 0 0 20px;
          color: var(--muted);
        }
        .card {
          background: var(--card);
          border: 1px solid var(--border);
          border-radius: 12px;
          box-shadow: 0 8px 24px rgba(0,0,0,0.04);
          padding: 20px;
          margin-bottom: 16px;
        }
        textarea, input, button {
          font-size: 14px;
          font-family: inherit;
        }
        textarea {
          width: 100%;
          min-height: 140px;
          resize: vertical;
          border-radius: 10px;
          border: 1px solid var(--border);
          padding: 12px;
          background: #fff;
          transition: border 0.15s ease;
        }
        textarea:focus, input:focus, button:focus {
          outline: none;
          border-color: var(--accent);
          box-shadow: 0 0 0 2px var(--accent-soft);
        }
        .row {
          margin: 12px 0;
          display: flex;
          align-items: center;
          gap: 12px;
          flex-wrap: wrap;
        }
        label { color: var(--muted); }
        .btn {
          background: var(--accent);
          color: #fff;
          border: none;
          border-radius: 10px;
          padding: 10px 18px;
          cursor: pointer;
          font-weight: 600;
          transition: transform 0.1s ease, box-shadow 0.1s ease;
        }
        .btn:hover { box-shadow: 0 10px 24px rgba(37, 99, 235, 0.25); }
        .btn:active { transform: translateY(1px); }
        .stack { display: grid; gap: 16px; }
        .badge {
          display: inline-flex;
          align-items: center;
          gap: 6px;
          padding: 6px 10px;
          border-radius: 999px;
          background: var(--accent-soft);
          color: var(--accent);
          font-weight: 600;
          font-size: 12px;
        }
        .muted { color: var(--muted); font-size: 13px; }
        pre {
          background: #0f172a;
          color: #e5e7eb;
          padding: 12px;
          border-radius: 10px;
          overflow: auto;
          font-family: var(--mono);
          font-size: 13px;
          margin: 0;
        }
        table {
          width: 100%;
          border-collapse: collapse;
          border: 1px solid var(--border);
          border-radius: 10px;
          overflow: hidden;
        }
        th, td {
          padding: 8px 10px;
          border-bottom: 1px solid var(--border);
          font-size: 13px;
        }
        th { text-align: left; background: #f8fafc; }
        tr:last-child td { border-bottom: none; }
        .pill {
          display: inline-block;
          padding: 2px 8px;
          border-radius: 999px;
          background: #ecfeff;
          color: #0ea5e9;
          font-size: 12px;
          border: 1px solid #cffafe;
        }
        .flex-between {
          display: flex;
          align-items: center;
          justify-content: space-between;
          gap: 12px;
          flex-wrap: wrap;
        }
        .error { color: var(--error); font-weight: 600; }
        .success { color: var(--success); font-weight: 600; }
        .raw-toggle { font-size: 13px; color: var(--accent); cursor: pointer; }
      </style>
    </head>
    <body>
      <div class="shell">
        <h1>DuckDB NL→SQL Chatbot</h1>

        <div class="card stack">
          <div class="row">
            <label for="question" class="muted">Question</label>
          </div>
          <textarea id="question" placeholder="e.g., Top 5 start stations in London"></textarea>
          <div class="row">
            <label>Max rows: <input id="maxRows" type="number" value="50" min="1" max="500"/></label>
            <span class="muted">Tables allowed: london_bike_data, nyc_biking_data, joint_bike_data</span>
          </div>
          <div class="row">
            <button class="btn" onclick="send()">Ask</button>
            <span id="status" class="muted"></span>
          </div>
        </div>

        <div class="card stack">
          <div class="flex-between">
            <div class="badge">Response</div>
            <span class="raw-toggle" onclick="toggleRaw()">Toggle raw JSON</span>
          </div>
          <div class="stack">
            <div><strong>Answer:</strong> <span id="answer" class="muted">Awaiting request...</span></div>
            <div>
              <strong>SQL:</strong>
              <pre id="sql">(none)</pre>
            </div>
            <div>
              <strong>Data preview:</strong>
              <div id="preview-container" class="muted">No rows.</div>
            </div>
            <div id="raw" style="display:none;">
              <strong>Raw JSON:</strong>
              <pre id="output">Awaiting request...</pre>
            </div>
          </div>
        </div>
      </div>

      <script>
        function toggleRaw() {
          const el = document.getElementById('raw');
          el.style.display = el.style.display === 'none' ? 'block' : 'none';
        }

        function renderTable(rows, columns) {
          if (!rows || rows.length === 0) return '<div class="muted">No rows.</div>';
          const cols = columns && columns.length ? columns : Object.keys(rows[0] || {});
          let thead = cols.map(c => `<th>${c}</th>`).join('');
          let body = rows.map(r => {
            return '<tr>' + cols.map(c => `<td>${r[c] !== undefined ? r[c] : ''}</td>`).join('') + '</tr>';
          }).join('');
          return `<div style="overflow:auto;"><table><thead><tr>${thead}</tr></thead><tbody>${body}</tbody></table></div>`;
        }

        async function send() {
          const question = document.getElementById('question').value;
          const maxRows = parseInt(document.getElementById('maxRows').value, 10) || 50;
          const payload = { question, max_rows: maxRows };
          const status = document.getElementById('status');
          status.textContent = 'Running...';
          try {
            const res = await fetch('/ask', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify(payload)
            });
            const text = await res.text();
            let display = text;
            try {
              const obj = JSON.parse(text);
              display = JSON.stringify(obj, null, 2);
              document.getElementById('output').textContent = display;
              document.getElementById('answer').textContent = obj.answer || '(no answer)';
              document.getElementById('sql').textContent = obj.sql || '(no sql)';
              document.getElementById('preview-container').innerHTML = renderTable(obj.data_preview || [], obj.columns || []);
              status.textContent = 'Done';
              status.className = 'muted success';
            } catch (err) {
              display = text; // fallback to raw text
              document.getElementById('output').textContent = display;
              document.getElementById('answer').textContent = 'Could not parse response.';
              document.getElementById('sql').textContent = '(n/a)';
              document.getElementById('preview-container').innerHTML = '<div class="error">Parse error</div>';
              status.textContent = 'Error parsing response';
              status.className = 'muted error';
            }
          } catch (err) {
            document.getElementById('output').textContent = 'Error: ' + err;
            document.getElementById('answer').textContent = 'Request failed.';
            document.getElementById('sql').textContent = '(n/a)';
            document.getElementById('preview-container').innerHTML = '<div class="error">Request failed</div>';
            status.textContent = 'Request failed';
            status.className = 'muted error';
          }
        }
      </script>
    </body>
    </html>
    """


def run_sql(sql: str) -> pd.DataFrame:
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        return con.execute(sql).fetch_df()
    finally:
        con.close()


def summarize_df(df: pd.DataFrame) -> str:
    if df.empty:
        return "No rows returned."
    cols = ", ".join(df.columns.tolist())
    return f"Returned {len(df)} rows. Columns: {cols}."


@app.post("/ask", response_model=AskResponse)
def ask(body: AskRequest) -> AskResponse:
    if not body.question.strip():
        raise HTTPException(status_code=400, detail="Question is empty.")

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": body.question},
    ]
    try:
        completion = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=messages,
            temperature=0.2,
        )
    except Exception as e:  # pragma: no cover
        raise HTTPException(status_code=500, detail=f"LLM call failed: {e}")

    sql = extract_sql(completion.choices[0].message.content or "")
    ok, reason = validate_sql(sql, SCHEMA)
    if not ok:
        raise HTTPException(status_code=400, detail=f"SQL rejected: {reason}")

    try:
        df = run_sql(sql)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"SQL execution failed: {e}")

    preview = df.head(min(body.max_rows, 100)).to_dict(orient="records")
    answer_text = summarize_df(df)

    return AskResponse(
        answer=answer_text,
        sql=sql,
        data_preview=preview,
        columns=df.columns.tolist(),
    )


# For local dev: uvicorn chatbot_service:app --reload --port 8000


