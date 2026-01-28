import streamlit as st
import pandas as pd
from datasources.sqlserver import get_sqlserver_connection
from datasources.snowflake import connect_snowflake
from dotenv import load_dotenv
import os
from spark.schema_compare import get_tables, get_table_schema
from genai.sql_generator import get_ingestion_decision, classify_impacted_views_llm
from genai.llm_summary import generate_sql_documentation
from spark.schema_compare import get_file_schema
from spark.lineage import get_impacted_views_snowflake, get_view_definitions
from spark.ingest import insert_data
from openai import OpenAI
import re
import json
import time
import sqlite3
from spark.audit_logger import (
    init_app_action_log,
    init_db_change_log,
    log_app_action,
    log_db_change,
)

# Load environment variables first
st.session_state.setdefault("ingestion_mode", None)
st.session_state.setdefault("selected_table", None)
st.session_state.setdefault("decision", None)
st.session_state.setdefault("confirm", False)
st.session_state.setdefault("catalog_objects", None)
st.session_state.setdefault("catalog_overview", None)
st.session_state.setdefault("catalog_selected", None)
st.session_state.setdefault("catalog_selected_type", None)
st.session_state.setdefault("tables", None)


schema = None
table_name = None
dialect = None
load_dotenv()
init_app_action_log()
init_db_change_log()



api_key = st.secrets["OPENAI_API_KEY"]
# api_key = os.environ.get("OPENAI_API_KEY")

openai_client = OpenAI(
    api_key=api_key,
    timeout=30,
    max_retries=3  
)

st.set_page_config(
    page_title="ZeusAI SQL Ingestion",
    layout="wide"
)

st.title("🧠 ZeusAI-Driven DATA Ingestion")
import re

def find_downstream_views(conn, database, schema, object_name):

    sql = f"""
    SELECT TABLE_SCHEMA, TABLE_NAME, VIEW_DEFINITION
    FROM {database}.INFORMATION_SCHEMA.VIEWS
    """

    df = pd.read_sql(sql, conn)

    pattern = f"{schema}.{object_name}".upper()

    mask = df["VIEW_DEFINITION"].astype(str).str.upper().str.contains(
    pattern,
    na=False
)


    return df.loc[mask, ["TABLE_SCHEMA", "TABLE_NAME"]]
def find_downstream_procs(conn, database, schema, object_name):

    sql = f"""
    SELECT
        ROUTINE_SCHEMA,
        ROUTINE_NAME,
        ROUTINE_DEFINITION
    FROM INFORMATION_SCHEMA.ROUTINES
    WHERE ROUTINE_TYPE = 'PROCEDURE'
    """

    df = pd.read_sql(sql, conn)

    pattern = f"{schema}.{object_name}".upper()

    mask = df["ROUTINE_DEFINITION"].astype(str).str.upper().str.contains(
    pattern,
    na=False
)


    return df.loc[mask, ["ROUTINE_SCHEMA", "ROUTINE_NAME"]]

def extract_sql_lineage(sql_text):

    sql = sql_text.upper()

    tables = set()
    views = set()
    procedures = set()
    functions = set()

    # FROM / JOIN objects
    for m in re.findall(r"(?:FROM|JOIN)\s+([A-Z0-9_\.]+)", sql):
        tables.add(m)

    # CALL proc()
    for m in re.findall(r"CALL\s+([A-Z0-9_\.]+)", sql):
        procedures.add(m)

    # functions func(...)
    for m in re.findall(r"([A-Z_][A-Z0-9_]*)\s*\(", sql):
        if m not in ["SELECT", "FROM", "JOIN", "WHERE", "AND", "OR"]:
            functions.add(m)

    return {
        "tables": sorted(tables),
        "views": sorted(views),
        "procedures": sorted(procedures),
        "functions": sorted(functions),
    }


def save_to_sqlite(df, db_path="zeusai_results.db", table_name="pii_scan_results"):
    """
    Save DataFrame to SQLite.
    - Creates DB if not exists
    - Replaces table by default
    """
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()

def safe_json_loads(text: str):
    """
    Safely extract and parse the FIRST valid JSON object or array
    from an LLM response.
    """

    if not text:
        raise ValueError("❌ Empty LLM response")

    # Remove markdown fences if present
    text = text.replace("```json", "").replace("```", "").strip()

    # Try direct parse first
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Regex: extract JSON array OR object
    json_pattern = re.compile(
        r"(\[\s*{.*?}\s*\]|\{\s*.*?\s*\})",
        re.DOTALL
    )

    matches = json_pattern.findall(text)

    if not matches:
        raise ValueError("❌ No valid JSON found in LLM output")

    # Try parsing matches one by one
    for candidate in matches:
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            continue

    raise ValueError("❌ Found JSON-like text but failed to parse it")

    
# def escape_sqlserver_columns(cols):
#     return [f"[{c}]" for c in cols]

def rebuild_insert_sql(table_name, columns, dialect):
    if dialect == "sqlserver":
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join(["?"] * len(columns))
    else:  # snowflake
        col_list = ", ".join(f'"{c}"' for c in columns)
        placeholders = ", ".join(["%s"] * len(columns))

    return f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"

def build_risk_df(impacted_views_df, llm_result_json):
    risk_df = pd.DataFrame(llm_result_json)

    final_df = impacted_views_df.merge(
        risk_df,
        on="view_name",
        how="left"
    )

    # final_df = final_df.sort_values("precedence")

    return final_df

def fetch_top_n_rows(conn, table_name, dialect, n=10):
    cursor = conn.cursor()

    if dialect == "sqlserver":
        sql = f"SELECT TOP {n} * FROM {table_name}"
    else:  # snowflake
        sql = f"SELECT * FROM {table_name} LIMIT {n}"

    cursor.execute(sql)
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]

    return pd.DataFrame(rows, columns=columns)
# ================================
# 🔎 OBJECT TIMESTAMPS
# ================================

def get_object_timestamps_sqlserver(conn):

    sql = """
    SELECT
        s.name AS schema,
        o.name AS object_name,
        o.type_desc,
        o.create_date,
        o.modify_date
    FROM sys.objects o
    JOIN sys.schemas s
        ON o.schema_id = s.schema_id
    WHERE o.type IN ('U','V','P','FN','IF','TF');
    """

    return pd.read_sql(sql, conn)


def get_object_timestamps_snowflake(conn, database):

    tables_sql = f"""
    SELECT
        TABLE_SCHEMA AS schema,
        TABLE_NAME AS name,
        CREATED AS create_date,
        LAST_ALTERED AS modify_date
    FROM {database}.INFORMATION_SCHEMA.TABLES
    """

    df_tables = pd.read_sql(tables_sql, conn)

    try:

        procs_sql = f"""
        SELECT
            PROCEDURE_SCHEMA AS schema,
            PROCEDURE_NAME AS name,
            CREATED AS create_date,
            LAST_ALTERED AS modify_date
        FROM {database}.INFORMATION_SCHEMA.PROCEDURES
        """

        df_procs = pd.read_sql(procs_sql, conn)

    except Exception:
        df_procs = pd.DataFrame(
            columns=["schema", "name", "create_date", "modify_date"]
        )

    return pd.concat(
        [df_tables, df_procs],
        ignore_index=True,
    )


def get_object_row_counts(conn, catalog_df, dialect):

    cursor = conn.cursor()
    results = []

    for _, row in catalog_df.iterrows():

        obj = row["full_name"]
        obj_type = row["object_type"]

        cnt = None

        if obj_type == "TABLE":
            sql = f"SELECT COUNT(*) FROM {obj}"

            try:
                cursor.execute(sql)
                cnt = cursor.fetchone()[0]
            except:
                pass

        results.append(
            {
                "full_name": obj,
                "object_type": obj_type,
                "rows": cnt,
                "create_date": row.get("create_date"),
                "modify_date": row.get("modify_date"),
            }
        )

    return pd.DataFrame(results)

def build_view_html_report(
    view_name,
    modify_date,
    create_date,
    sql_text,
    llm_doc,
    lineage,
    downstream_views=None,
    downstream_procs=None,
):
    def list_block(title, items):

        if items is None:
            return "<p><i>None</i></p>"

        

        if isinstance(items, pd.DataFrame):

            if items.empty:
                return "<p><i>None</i></p>"

            rows = []

            for _, r in items.iterrows():
                rows.append(".".join(str(x) for x in r.values))

            li = "".join(f"<li>{x}</li>" for x in rows)

            return f"<ul>{li}</ul>"

        # list / tuple / set
        if isinstance(items, (list, tuple, set)):

            if len(items) == 0:
                return "<p><i>None</i></p>"

            li = "".join(f"<li>{x}</li>" for x in items)

            return f"<ul>{li}</ul>"

        return f"<p>{items}</p>"



    html = f"""
    <html>
    <head>
        <title>{view_name} – Data Catalog</title>
        <style>
            body {{ font-family: Arial; padding: 30px; }}
            h1 {{ color:#2b6cb0; }}
            h2 {{ margin-top:25px; }}
            pre {{
                background:#f4f4f4;
                padding:12px;
                border-radius:6px;
                overflow-x:auto;
            }}
            table {{
                border-collapse: collapse;
                width: 100%;
            }}
            th, td {{
                border: 1px solid #ddd;
                padding: 8px;
            }}
            th {{
                background-color: #f0f2f6;
            }}
        </style>
    </head>

    <body>

    <h1>📄 View Catalog Report</h1>

    <h2>{view_name}</h2>

    <b>Created:</b> {create_date}<br>
    <b>Last Modified:</b> {modify_date}<br>

    <h2>🧠 Description</h2>
    <pre>{llm_doc or "N/A"}</pre>

    <h2>🧬 SQL Definition</h2>
    <pre>{sql_text}</pre>

    <h2>🌐 Lineage</h2>

    <h3>Tables Used</h3>
    {list_block("Tables", lineage.get("tables", []))}

    <h3>Views Used</h3>
    {list_block("Views", lineage.get("views", []))}

    <h3>Procedures Used</h3>
    {list_block("Procs", lineage.get("procedures", []))}

    <h3>Functions Used</h3>
    {list_block("Functions", lineage.get("functions", []))}

    <h2>🔁 Relations</h2>

    <h3>Downstream Views</h3>
    {list_block("Downstream Views", downstream_views)}

    <h3>Downstream Procedures</h3>
    {list_block("Downstream Procs", downstream_procs)}

    </body>
    </html>
    """

    return html

def save_catalog_to_sqlite(
    df,
    source_object,
    object_type,
    db_path="zeusai_results.db"
):

    conn = sqlite3.connect(db_path)

    df["source_object"] = source_object
    df["object_type"] = object_type

    df.to_sql(
        "data_catalog",
        conn,
        if_exists="append",
        index=False
    )

    conn.close()


def get_views(conn, dialect, schema=None, database=None):

    cursor = conn.cursor()

    if dialect == "sqlserver":

        sql = """
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.VIEWS
        """

        cursor.execute(sql)
        rows = cursor.fetchall()

        return pd.DataFrame(rows, columns=["schema", "name"])

    else:  # snowflake

        sql = f"SHOW VIEWS IN SCHEMA {database}.{schema}"

        cursor.execute(sql)

        rows = cursor.fetchall()
        cols = [c[0].lower() for c in cursor.description]

        df = pd.DataFrame(rows, columns=cols)

        # --- normalize column names ---
        col_map = {}

        if "schema_name" in df.columns:
            col_map["schema_name"] = "schema"

        if "name" in df.columns:
            col_map["name"] = "name"
        elif "view_name" in df.columns:
            col_map["view_name"] = "name"

        df = df.rename(columns=col_map)

        # Safety check
        required = {"schema", "name"}

        if not required.issubset(df.columns):
            raise RuntimeError(
                f"SHOW VIEWS returned unexpected columns: {df.columns.tolist()}"
            )

        return df[["schema", "name"]]



    # rows = cursor.fetchall()

    # return pd.DataFrame(rows, columns=["schema", "name"])



def get_procedures(conn, dialect):

    cursor = conn.cursor()

    if dialect == "sqlserver":
        sql = """
        SELECT SPECIFIC_SCHEMA, SPECIFIC_NAME
        FROM INFORMATION_SCHEMA.ROUTINES
        WHERE ROUTINE_TYPE = 'PROCEDURE'
        """
    else:
        sql = """
        SELECT PROCEDURE_SCHEMA, PROCEDURE_NAME
        FROM INFORMATION_SCHEMA.PROCEDURES
        """

    cursor.execute(sql)

    rows = cursor.fetchall()

    return pd.DataFrame(rows, columns=["schema", "name"])


def detect_pii_llm(openai_client, table_name, sample_payload):
    prompt = f"""
You are a data privacy expert.

Analyze the following table samples and identify PII columns.

TABLE:
{table_name}

COLUMN SAMPLES:
{json.dumps(sample_payload, indent=2)}

RULES:
- Identify PII based on DATA PATTERNS, not column names alone
- Mark as:
  - PII
  - POSSIBLE_PII
  - NON_PII
- Provide a short reason
- Be conservative

RETURN ONLY VALID JSON.

FORMAT:
{{
  "columns": [
    {{
      "column_name": "EMAIL",
      "pii_type": "EMAIL | PHONE | NAME | ID | FINANCIAL | DOB | ADDRESS | OTHER | NONE | SSN | AADHAAR | PAN",
      "classification": "PII | POSSIBLE_PII | NON_PII",
      "confidence": "HIGH | MEDIUM | LOW",
      "reason": "Short explanation"
    }}
  ]
}}
"""
    response = openai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    return response.choices[0].message.content

db_type = st.radio(
    "Select Database Type",
    ["SQL Server", "Snowflake"]
)

if db_type == "SQL Server":
    st.subheader("🔌 SQL Server Connection")

    col1, col2 = st.columns(2)

    with col1:
        server = st.text_input("Server")
        database = st.text_input("Database")
        driver = st.text_input(
            "ODBC Driver",
            "ODBC Driver 17 for SQL Server"
        )

    with col2:
        user = st.text_input("User")
        password = st.text_input(
            "Password",
            type="password"
        )

if db_type == "Snowflake":
    st.subheader("❄️ Snowflake Connection")

    col1, col2 = st.columns(2)

    with col1:
        account = st.text_input(
            "Account (without .snowflakecomputing.com)"
        )
        user = st.text_input("User")
        password = st.text_input(
            "Password",
            type="password"
        )
        
        role = st.text_input("Role (optional)")

    with col2:
        warehouse = st.text_input("Warehouse")
        database = st.text_input("Database")
        schema = st.text_input("Schema")
        


if st.button("Connect"):
    try:
        if db_type == "SQL Server":
            conn = get_sqlserver_connection(
                server, database, user, password, driver
            )
            dialect = "sqlserver"
        else:
            conn = connect_snowflake(
                account, user, password,
                warehouse, database, schema, role or None
            )
            dialect = "snowflake"

        st.session_state["conn"] = conn
        st.session_state["db_dialect"] = dialect

        tables_df = get_tables(conn, dialect)
        st.session_state["tables"] = tables_df

        views_df = get_views(conn, dialect, schema, database)
        procs_df = get_procedures(conn, dialect)

        tables_df["object_type"] = "TABLE"
        views_df["object_type"] = "VIEW"
        procs_df["object_type"] = "PROC"

        tables_df["full_name"] = (
            tables_df["TABLE_SCHEMA"] + "." + tables_df["TABLE_NAME"]
        )

        views_df["full_name"] = (
            views_df["schema"] + "." + views_df["name"]
        )

        procs_df["full_name"] = (
            procs_df["schema"] + "." + procs_df["name"]
        )

        catalog_df = pd.concat(
            [
                tables_df[["full_name", "object_type"]],
                views_df[["full_name", "object_type"]],
                procs_df[["full_name", "object_type"]],
            ],
            ignore_index=True,
        )
        # ---------------------------------------
# 🔥 ADD CREATE / MODIFY TIMESTAMPS
# ---------------------------------------

        if dialect == "sqlserver":

            ts_df = get_object_timestamps_sqlserver(conn)
            st.write("DEBUG ts_df columns:", ts_df.columns.tolist())
            st.write("DEBUG ts_df preview:", ts_df.head())


            ts_df["full_name"] = ts_df["schema"] + "." + ts_df["object_name"]


            catalog_df = catalog_df.merge(
                ts_df[["full_name", "create_date", "modify_date"]],
                on="full_name",
                how="left"
            )

        elif dialect == "snowflake":

            ts_df = get_object_timestamps_snowflake(conn, database)

            # -----------------------------
            # NORMALIZE TIMESTAMP DF
            # -----------------------------

            ts_df.columns = [c.lower() for c in ts_df.columns]

            rename_map = {}

            if "schema" in ts_df.columns:
                rename_map["schema"] = "schema"

            if "name" in ts_df.columns:
                rename_map["name"] = "name"

            if "create_date" in ts_df.columns:
                rename_map["create_date"] = "create_date"

            if "modify_date" in ts_df.columns:
                rename_map["modify_date"] = "modify_date"

            ts_df = ts_df.rename(columns=rename_map)

            # 🚫 Remove INFORMATION_SCHEMA noise
            ts_df = ts_df[ts_df["schema"].str.upper() != "INFORMATION_SCHEMA"]

            # Build full name
            ts_df["full_name"] = ts_df["schema"] + "." + ts_df["name"]

            # -----------------------------
            # MERGE ONCE INTO CATALOG
            # -----------------------------

            catalog_df = catalog_df.merge(
                ts_df[["full_name", "create_date", "modify_date"]],
                on="full_name",
                how="left",
            )

        # st.stop()
        # st.write("🧪 TABLES:", len(tables_df))
        # st.write("🧪 VIEWS:", len(views_df))
        # st.write("🧪 PROCS:", len(procs_df))
        # st.dataframe(catalog_df)


        st.session_state.pop("catalog_overview", None)
        st.session_state["catalog_objects"] = catalog_df
        st.success(f"✅ Connected to {db_type}")

        log_app_action(
            "CONNECT_DB",
            message="Database connection established",
            details={
                "db_type": db_type,
                "database": database,
                "schema": schema if db_type == "Snowflake" else None,
            },
        )

    except Exception as e:
        st.exception(e)
        raise


with st.container():
    col1, col2 = st.columns([1, 9])

    with col1:
        if st.button("🔄 Refresh"):

            # 🔥 Reset workflow state
            keys_to_reset = [
                "ingestion_mode",
                "selected_table",
                "decision",
                "confirm",
                "df_file",
                "pii_scan_results",
                "impacted_views"
            ]
        
            for key in keys_to_reset:
                st.session_state.pop(key, None)
        
            # 🔁 RELOAD TABLES FROM DB (THIS IS THE KEY)
            conn = st.session_state.get("conn")
            dialect = st.session_state.get("db_dialect")
        
            if conn and dialect:
                tables_df = get_tables(conn, dialect)
                st.session_state["tables"] = tables_df

        
                if dialect == "sqlserver":
                    tables_df["full_name"] = (
                        tables_df["TABLE_SCHEMA"] + "." + tables_df["TABLE_NAME"]
                    )
                else:
                    tables_df["full_name"] = tables_df["TABLE_NAME"]
        
                st.session_state["tables"] = tables_df
        
            st.success("Workflow reset. Metadata refreshed.")
            st.session_state.pop("catalog_overview", None)
            st.session_state.pop("catalog_objects", None)

            st.rerun()

# if st.session_state.get("catalog_objects") is None:
#     st.info("ℹ️ Connect to a database to load schema catalog.")
#     st.stop()


if "conn" in st.session_state:
    st.subheader("🧭 Select Operation")

    ingestion_mode = st.radio(
        "Choose ingestion type",
        [
            "DB Change",
            "Ingest into Existing Table",
            "Create New Table & Ingest",
            
            "Change Detection"
            # "Detect PII from Existing Table"
        ],
        index=None
    )

    # ingestion_mode = st.radio(
    #     "Choose ingestion type",
    #     [
    #         "DB Change",
    #         "Change Detection",

    #     ],
    #     index=None
    # )

    st.session_state["ingestion_mode"] = ingestion_mode
    if ingestion_mode:
        log_app_action(
            "SELECT_MODE",
            message="User selected ingestion mode",
            details={"mode": ingestion_mode},
        )

# =========================
# 3. TABLE SELECTION
# =========================
# =========================
# PII DETECTION — ALL TABLES
# =========================
if st.session_state.get("ingestion_mode") == "DB Change":
    # 🔄 rebuild catalog when entering DB Change
    if "catalog_objects" not in st.session_state or st.session_state["catalog_objects"] is None:
        st.warning("⚠️ Schema catalog not loaded. Please reconnect.")
        st.stop()

    st.session_state.pop("catalog_overview", None)

    # ============================
    # 📚 SCHEMA CATALOG — CLEAN GRID
    # ============================

    st.subheader("📚 Schema Catalog")
    st.caption("Click a table to view its metadata 👇")

    conn = st.session_state["conn"]
    dialect = st.session_state["db_dialect"]
    tables_df = st.session_state["tables"]

    # Cache overview once
    catalog_df = st.session_state.get("catalog_objects")


    if "catalog_overview" not in st.session_state:
        overview_df = get_object_row_counts(
            conn,
            catalog_df,
            dialect
        )

        st.session_state["catalog_overview"] = overview_df

    overview_df = st.session_state.get("catalog_overview")

    if overview_df is None or overview_df.empty:
        st.info("ℹ️ No catalog objects loaded yet. Click Connect.")
        st.stop()
    # st.write("DEBUG overview_df columns:", overview_df.columns.tolist())
    # st.write("DEBUG overview_df sample:", overview_df.head())
    # st.stop()

    tables_only = overview_df[overview_df["object_type"] == "TABLE"]
    views_only = overview_df[overview_df["object_type"] == "VIEW"]
    procs_only = overview_df[overview_df["object_type"] == "PROC"]


    # # ---- GRID LAYOUT ----
    # cols_per_row = 3

    # rows = [
    #     overview_df.iloc[i:i + cols_per_row]
    #     for i in range(0, len(overview_df), cols_per_row)
    # ]

    # for r_idx, chunk in enumerate(rows):

    #     cols = st.columns(cols_per_row)

    #     for c_idx, (_, row) in enumerate(chunk.iterrows()):

    #         with cols[c_idx]:

    #             clicked = st.button(
    #                 f"📦 {row['full_name']}\n\n"
    #                 f"Rows: {row['rows']}",
    #                 key=f"obj_{row['full_name']}",
    #                 use_container_width=True,
    #             )


    #             if clicked:
    #                 st.session_state["catalog_selected"] = row["full_name"]
    #                 st.session_state["catalog_selected_type"] = row["object_type"]

    #                 st.rerun()
    # st.write("DEBUG catalog_objects:", st.session_state.get("catalog_objects"))
    # st.write("DEBUG overview_df:", overview_df)

    if not tables_only.empty:

        st.markdown("## 📦 Tables")

        cols_per_row = 3

        rows = [
            tables_only.iloc[i:i + cols_per_row]
            for i in range(0, len(tables_only), cols_per_row)
        ]

        for chunk in rows:

            cols = st.columns(cols_per_row)

            for i, row in chunk.iterrows():

                with cols[list(chunk.index).index(i)]:

                    clicked = st.button(
                        f"📦 {row['full_name']}\n\n"
                        f"Rows: {row['rows']}\n\n"
                        f"Last Modified: {row.get('modify_date', 'N/A')}",
                        key=f"tbl_{row['full_name']}",
                        use_container_width=True,
                    )

                    if clicked:
                        st.session_state["catalog_selected"] = row["full_name"]
                        st.session_state["catalog_selected_type"] = "TABLE"
                        st.rerun()

    # ----------------------------
    # 👁️ VIEWS
    # ----------------------------

    if not views_only.empty:

        st.markdown("## 👁️ Views")

        rows = [
            views_only.iloc[i:i + cols_per_row]
            for i in range(0, len(views_only), cols_per_row)
        ]

        for chunk in rows:

            cols = st.columns(cols_per_row)

            for i, row in chunk.iterrows():

                with cols[list(chunk.index).index(i)]:

                    clicked = st.button(
                        f"👁️ {row['full_name']}\n\n"
                        f"Last Modified: {row.get('modify_date', 'N/A')}",
                        key=f"view_{row['full_name']}",
                        use_container_width=True,
                    )

                    if clicked:
                        st.session_state["catalog_selected"] = row["full_name"]
                        st.session_state["catalog_selected_type"] = "VIEW"
                        st.rerun()

    # ----------------------------
    # ⚙️ PROCS
    # ----------------------------

    if not procs_only.empty:

        st.markdown("## ⚙️ Procedures")

        rows = [
            procs_only.iloc[i:i + cols_per_row]
            for i in range(0, len(procs_only), cols_per_row)
        ]

        for chunk in rows:

            cols = st.columns(cols_per_row)

            for i, row in chunk.iterrows():

                with cols[list(chunk.index).index(i)]:

                    clicked = st.button(
                        f"⚙️ {row['full_name']}",
                        key=f"proc_{row['full_name']}",
                        use_container_width=True,
                    )

                    if clicked:
                        st.session_state["catalog_selected"] = row["full_name"]
                        st.session_state["catalog_selected_type"] = "PROC"
                        st.rerun()

    if st.session_state.get("catalog_selected"):

        st.divider()

        selected = st.session_state["catalog_selected"]

        st.subheader(f"🗄 {selected}")

        dialect = st.session_state["db_dialect"]
        conn = st.session_state["conn"]

        parts = selected.split(".")

        if len(parts) == 3:
            db_name, schema, table_name = parts
        elif len(parts) == 2:
            schema, table_name = parts
            db_name = database
        else:
            table_name = parts[0]
            schema = schema
            db_name = database

        obj_type = st.session_state["catalog_selected_type"]
        # --------------------------------
        # 📘 FETCH VIEW DEFINITION
        # --------------------------------

        view_sql = None

        if obj_type == "VIEW":

            if dialect == "sqlserver":

                sql = f"""
                SELECT definition
                FROM sys.sql_modules m
                JOIN sys.views v ON m.object_id = v.object_id
                WHERE SCHEMA_NAME(v.schema_id) = '{schema}'
                AND v.name = '{table_name}'
                """

            else:  # snowflake

                sql = f"""
                SELECT GET_DDL('VIEW',
                    '{db_name}.{schema}.{table_name}')
                """

            df_def = pd.read_sql(sql, conn)

            view_sql = df_def.iloc[0, 0]
        llm_doc = None

        if obj_type == "VIEW" and view_sql:

            with st.spinner("🧠 Generating technical summary with GenAI..."):

                llm_doc = generate_sql_documentation(
                    object_name=selected,
                    object_type="view",
                    object_sql=view_sql
                )
                log_app_action(
                    "GENERATE_VIEW_SUMMARY",
                    object_name=selected,
                    object_type="VIEW",
                )

        # =====================================
    # 📘 VIEW DOCUMENTATION + LINEAGE UI
    # =====================================

            st.markdown("## 🧠 Description")
            st.markdown(llm_doc or "No description generated.")

            st.markdown("## 🌐 Lineage")

            lineage = extract_sql_lineage(view_sql)
            log_app_action(
                "EXTRACT_LINEAGE",
                object_name=selected,
                object_type="VIEW",
                details=lineage,
            )


            st.markdown("### Tables Used")
            st.write(lineage.get("tables", []))

            st.markdown("### Views Used")
            st.write(lineage.get("views", []))

            st.markdown("### Procedures Used")
            st.write(lineage.get("procedures", []))

            st.markdown("### Functions Used")
            st.write(lineage.get("functions", []))
            downstream_views = None
            downstream_procs = None

            st.markdown("## 🔁 Relations")

            st.markdown("### Called by Views")
            downstream_views = find_downstream_views(conn, db_name, schema, table_name)
            if downstream_views is not None and not downstream_views.empty:
                st.dataframe(downstream_views)
            else:
                st.write("None")


            st.markdown("### Called by Procedures")

            if dialect == "sqlserver":

                downstream_procs = find_downstream_procs(
                    conn,
                    db_name,
                    schema,
                    table_name
                )

                if downstream_procs is not None and not downstream_procs.empty:
                    st.dataframe(downstream_procs)
                else:
                    st.write("None")

            else:
                st.write("Not applicable for Snowflake.")



        # ----------------------------
        # 📥 DOWNLOAD HTML REPORT
        # ----------------------------

            html_report = build_view_html_report(
                view_name=selected,
                modify_date=row.get("modify_date"),
                create_date=row.get("create_date"),
                sql_text=view_sql,
                llm_doc=llm_doc,
                lineage=lineage,
                downstream_views=downstream_views,
                downstream_procs=downstream_procs,
            )

            st.download_button(
                label="📥 Download View Report (HTML)",
                data=html_report,
                file_name=f"{selected.replace('.', '_')}_catalog.html",
                mime="text/html",
            )




        if obj_type in ["TABLE", "VIEW"]:

            meta_df = get_table_schema(
                conn,
                schema,
                table_name,
                dialect
            )

        else:  # PROC

            if dialect == "sqlserver":

                meta_sql = f"""
                SELECT
                    PARAMETER_NAME as column_name,
                    DATA_TYPE as data_type,
                    CHARACTER_MAXIMUM_LENGTH as length
                FROM INFORMATION_SCHEMA.PARAMETERS
                WHERE SPECIFIC_SCHEMA = '{schema}'
                AND SPECIFIC_NAME = '{table_name}'
                """

            else:  # snowflake


        # Snowflake-safe: DESCRIBE PROCEDURE
                current_db = database  # already captured from UI

                meta_sql = f"DESCRIBE PROCEDURE {current_db}.{schema}.{table_name}()"


                meta_df = pd.read_sql(meta_sql, conn)

                # Normalize DESCRIBE output
                meta_df = meta_df.rename(
                    columns={
                        "name": "column_name",
                        "type": "data_type"
                    }
                )

                meta_df = meta_df[["column_name", "data_type"]]
                meta_df["length"] = None



            meta_df = pd.read_sql(meta_sql, conn)

            if "length" not in meta_df:
                meta_df["length"] = None



        # ----------------------------
        # 🔥 Normalize / enrich columns
        # ----------------------------

        meta_df.columns = [c.lower() for c in meta_df.columns]

        rename_map = {
            "column_name": "column_name",
            "data_type": "data_type",
            "character_maximum_length": "length",
            "is_nullable": "nullable"
        }

        meta_df = meta_df.rename(columns=rename_map)

        # Length cleanup
        if "length" not in meta_df:
            meta_df["length"] = None

        # Constraint flag
        meta_df["constraint"] = meta_df.apply(
            lambda r: "YES"
            if any(
                x in str(r).upper()
                for x in ["PRIMARY", "FOREIGN", "UNIQUE"]
            )
            else "NO",
            axis=1
        )

        # Editable columns
        meta_df["pii"] = False
        meta_df["tag"] = ""

        # Limit tag length to 20
        meta_df["tag"] = meta_df["tag"].astype(str).str[:20]

        display_cols = [
            "column_name",
            "data_type",
            "length",
            "constraint",
            "pii",
            "tag"
        ]
        show_editor = not (
            st.session_state.get("ingestion_mode") == "DB Change"
            and obj_type in ["VIEW", "PROC"]
        )


        if show_editor:

            editable_df = st.data_editor(
                meta_df[display_cols],
                num_rows="fixed",
                use_container_width=True,
                column_config={
                    "pii": st.column_config.CheckboxColumn(
                        "PII"
                    ),
                    "tag": st.column_config.TextColumn(
                        "Tag",
                        max_chars=20
                    ),
                }
            )

            # ----------------------------
            # 💾 SAVE TO SQLITE
            # ----------------------------

            if st.button("💾 Save Catalog Metadata"):

                save_catalog_to_sqlite(
                    editable_df,
                    source_object=selected,
                    object_type=st.session_state["catalog_selected_type"]
                )


                st.success("✅ Metadata saved to SQLite catalog")
                log_app_action(
                    "SAVE_CATALOG_METADATA",
                    object_name=selected,
                    object_type=st.session_state["catalog_selected_type"],
                )



if st.session_state.get("ingestion_mode") == "Detect PII from Existing Table":

    st.subheader("🔐 PII Scan Across All Tables")
    st.session_state.pop("pii_scan_results", None)
    if st.button("🚨 Scan All Tables for PII"):
        pii_results = []

        tables_df = st.session_state["tables"]
        conn = st.session_state["conn"]
        dialect = st.session_state["db_dialect"]

        progress = st.progress(0)
        total = len(tables_df)

        for idx, row in tables_df.iterrows():
            table_name = row["full_name"]

            try:
                # 1️⃣ Fetch top 10 rows
                sample_df = fetch_top_n_rows(
                    conn=conn,
                    table_name=table_name,
                    dialect=dialect,
                    n=10
                )

                if sample_df.empty:
                    continue

                # 2️⃣ Build LLM payload
                sample_payload = {
                    col: (
                        sample_df[col]
                        .dropna()
                        .astype(str)
                        .head(5)
                        .tolist()
                    )
                    for col in sample_df.columns
                }

                # 3️⃣ Call LLM
                pii_raw = detect_pii_llm(
                    openai_client=openai_client,
                    table_name=table_name,
                    sample_payload=sample_payload
                )

                pii_json = safe_json_loads(pii_raw)

                # 4️⃣ Normalize results
                for col in pii_json.get("columns", []):
                    pii_results.append({
                        "table_name": table_name,
                        "column_name": col.get("column_name"),
                        "pii_type": col.get("pii_type"),
                        "classification": col.get("classification"),
                        "confidence": col.get("confidence"),
                        "reason": col.get("reason")
                    })
                time.sleep(0.3)

            except Exception as e:
                pii_results.append({
                    "table_name": table_name,
                    "column_name": None,
                    "pii_type": None,
                    "classification": "ERROR",
                    "confidence": "LOW",
                    "reason": str(e)
                })

            progress.progress((idx + 1) / total)

        st.session_state["pii_scan_results"] = pd.DataFrame(pii_results)

# if st.session_state["ingestion_mode"] and "tables" in st.session_state:
#         # st.subheader("📋 Select Table")
    
#         tables_df = st.session_state["tables"]
    
#         # 🔒 Always ensure full_name exists
#         # if "full_name" not in tables_df.columns:
#         #     tables_df["full_name"] = (
#         #         tables_df["TABLE_SCHEMA"] + "." + tables_df["TABLE_NAME"]
#         #     )
#         #     st.session_state["tables"] = tables_df  # update session state
    
#         if st.session_state.get("ingestion_mode") == "Ingest into Existing Table":
    
#             selected_table = st.selectbox(
#                 "Choose existing table",
#                 tables_df["full_name"]
#             )
#             # st.session_state["selected_table"] = selected_table
#         elif st.session_state.get("ingestion_mode") == "Create New Table & Ingest":
#             selected_table = st.text_input(
#                 "Enter new table name",
#                 placeholder="schema.new_table"
#             )
#             if not selected_table:
#                 st.info("ℹ️ Enter a table name to continue")
#                 st.stop()
    
#         if "selected_table" in locals():
#             st.session_state["selected_table"] = selected_table
selected_table = None  # 🔒 ALWAYS initialize

if st.session_state["ingestion_mode"] and "tables" in st.session_state:

    tables_df = st.session_state["tables"]

    if st.session_state["ingestion_mode"] == "Ingest into Existing Table":
        selected_table = st.selectbox(
            "Choose existing table",
            tables_df["full_name"]
        )

    elif st.session_state["ingestion_mode"] == "Create New Table & Ingest":
        selected_table = st.text_input(
            "Enter new table name",
            placeholder="schema.new_table"
        )
        if not selected_table:
            st.info("ℹ️ Enter a table name to continue")
            st.stop()

    if selected_table:
        st.session_state["selected_table"] = selected_table

    
        dialect = st.session_state["db_dialect"]
    
        if "selected_table" in locals():
            if dialect == "sqlserver":
                schema, table_name = selected_table.split(".")
            else:
                schema = None
                table_name = selected_table
    
# # =========================
# # PII DETECTION MODE (READ-ONLY)
# # =========================
# if st.session_state.get("ingestion_mode") == "Detect PII from Existing Table":

#     st.subheader("🔐 PII Detection (Top 10 Rows)")

#     if st.button("🔍 Analyze PII"):
#         try:
#             sample_df = fetch_top_n_rows(
#                 conn=st.session_state["conn"],
#                 table_name=st.session_state["selected_table"],
#                 dialect=st.session_state["db_dialect"],
#                 n=10
#             )

#             if sample_df.empty:
#                 st.warning("No data found in table.")
#                 st.stop()

#             # Prepare payload for LLM
#             sample_payload = {
#                 col: sample_df[col]
#                 .dropna()
#                 .astype(str)
#                 .head(5)
#                 .tolist()
#                 for col in sample_df.columns
#             }

#             pii_raw = detect_pii_llm(
#                 openai_client=openai_client,
#                 table_name=st.session_state["selected_table"],
#                 sample_payload=sample_payload
#             )

#             pii_result = safe_json_loads(pii_raw)
#             st.session_state["pii_result"] = pii_result

#         except Exception as e:
#             st.error(f"❌ PII detection failed: {e}")


# =========================
# 4. FILE UPLOAD
# =========================
if (
    st.session_state.get("selected_table")
    and st.session_state.get("ingestion_mode") != "Detect PII from Existing Table"
):

    st.subheader("📤 Upload File to ingest")
    
    uploaded_file = st.file_uploader(
        "Upload CSV or Excel",
        type=["csv", "xlsx"]
    )
    
    if uploaded_file and st.session_state.get("selected_table"):
        # Force ALL data to string (VARCHAR rule)
        if uploaded_file.name.endswith(".csv"):
            df_file = pd.read_csv(uploaded_file, dtype=str)
        else:
            df_file = pd.read_excel(uploaded_file, dtype=str)
    
        df_file = df_file.astype(str)
    
        file_schema = pd.DataFrame({
            "COLUMN_NAME": df_file.columns,
            "DATA_TYPE": ["varchar"] * len(df_file.columns)
        })
    
        conn = st.session_state["conn"]
    
        # 🔥 KEY LOGIC
        if st.session_state["ingestion_mode"] == "Create New Table & Ingest":
            db_schema = pd.DataFrame(columns=["COLUMN_NAME", "DATA_TYPE"])
        else:
            db_schema = get_table_schema(
                conn,
                schema,
                table_name,
                dialect
            )
            db_schema["DATA_TYPE"] = "varchar"
    
    # =========================
    # 5. SHOW SCHEMA DETAILS
    # =========================
        st.subheader("📊 Data Catalog")

        if st.session_state["ingestion_mode"] == "Create New Table & Ingest":
            # ✅ Only file schema
            st.markdown("### 📁 Catalog from File")
            st.dataframe(file_schema)
            st.metric("File Column Count", len(file_schema))

        else:
            # ✅ Comparison mode
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("### 🗄 Catalog from DB")
                st.dataframe(db_schema)
                st.metric("DB Column Count", len(db_schema))

            with col2:
                st.markdown("### 📁 Catalog from File")
                st.dataframe(file_schema)
                st.metric("File Column Count", len(file_schema))


        if (
            db_type == "Snowflake"
            and st.session_state["ingestion_mode"] == "Ingest into Existing Table"
            and st.session_state.get("selected_table")
        ):
            impacted_views = get_impacted_views_snowflake(
                conn=st.session_state["conn"],
                database=database,
                schema="PUBLIC",
                table=table_name
            )
        
            st.session_state["impacted_views"] = impacted_views


    # =========================
    # 6. GENAI DECISION
    # =========================
    if (
    st.session_state.get("ingestion_mode") != "Detect PII from Existing Table"
    and st.button("🤖 Ask Zeus")
):

        decision_raw = get_ingestion_decision(openai_client,
                db_schema,
                file_schema,
                st.session_state["selected_table"],
                dialect,
                st.session_state["ingestion_mode"]
            )

        decision = safe_json_loads(decision_raw)
        st.session_state["decision"] = decision
        st.session_state["df_file"] = df_file

# =========================
# 7. SHOW GENAI SQL (PREVIEW)
# =========================

if st.session_state.get("ingestion_mode") and st.session_state.get("decision"):
    decision = st.session_state["decision"]
    target_table = st.session_state["selected_table"]

    # st.subheader("🔍 GenAI Suggested Actions")

    st.write("**Target Table:**", target_table)
    st.write("**Action:**", decision["action"])

    if decision.get("create_sql"):
        st.markdown("### 🆕 CREATE TABLE")
        for sql in decision["create_sql"]:
            st.code(sql, language="sql")

    if decision.get("alter_sql"):
        st.markdown("### 🛠 ALTER TABLE SQL")
        for sql in decision["alter_sql"]:
            st.code(sql, language="sql")

    st.markdown("### 📥 INSERT SQL")
    st.code(decision["insert_sql"], language="sql")

    # =========================
    # 8. USER CONFIRMATION
    # =========================
if "impacted_views" in st.session_state:
    st.subheader("🔎 Downstream Impact Analysis")
    
    df = st.session_state["impacted_views"]

    # 🔥 FIX: normalize column names
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    st.session_state["impacted_views"] = df

    if df.empty:
        st.success("✅ No downstream views will be impacted.")
    else:
        st.warning(
            f"⚠️ {len(df)} downstream object(s) will be affected by this ingestion"
        )

        # 1️⃣ Fetch view SQL (NOW THIS WORKS)
        view_defs = get_view_definitions(
            conn=st.session_state["conn"],
            database=database,
            schema="PUBLIC",
            view_names=df["view_name"].tolist()
        )

        # 2️⃣ Send to LLM
        llm_raw = classify_impacted_views_llm(
            openai_client=openai_client,
            views_df=view_defs,
            base_table=table_name
        )

        llm_result = safe_json_loads(llm_raw)
        risk_df = pd.DataFrame(llm_result)

        # 3️⃣ Merge + enforce precedence ordering
        final_df = (
            df.merge(risk_df, on="view_name", how="left")
        )

        st.dataframe(final_df)

        st.caption(
            "Risk is classified based on join type and SELECT usage. "
            "Precedence determines severity."
        )

if st.session_state.get("ingestion_mode") and st.session_state.get("decision"):

    st.session_state["confirm"] = st.checkbox(
        "I confirm the above SQL should be executed ONLY on the selected table",
        value=st.session_state["confirm"]
    )

    if st.button(
        "🚀 Execute",
        disabled=not st.session_state["confirm"]
    ):
        # run_ingestion()
        decision = st.session_state["decision"]
        target_table = st.session_state["selected_table"]
        try:
            conn = st.session_state["conn"]
            cursor = conn.cursor()

            # ---------------------------
            # 1. Execute ALTER safely
            # ---------------------------
            # CREATE TABLE
            if decision["action"] == "CREATE_AND_INSERT":

                create_sql = decision.get("create_sql")
            
                # 🔥 Normalize create_sql into ONE string
                if isinstance(create_sql, list):
                    create_sql = "\n".join(create_sql)
            
                if not create_sql or not create_sql.strip():
                    st.error("❌ Empty CREATE TABLE SQL generated")
                    st.stop()
            
                create_sql = re.sub(
                    r"\bRETURNS\s+VARCHAR\b",
                    "",
                    create_sql,
                    flags=re.IGNORECASE
                ).strip().rstrip(";")
            
                # Safety check
                if not create_sql.lower().startswith("create table"):
                    st.error("❌ Unsafe CREATE SQL detected")
                    st.code(create_sql, language="sql")
                    st.stop()
            
                st.write("🧪 Executing CREATE TABLE")
                st.code(create_sql, language="sql")
            
                cursor.execute(create_sql)
                log_db_change(
                    db_type=dialect,
                    database=database,
                    schema=schema,
                    table_name=target_table,
                    operation="CREATE_TABLE",
                    message="Table created via ZeusAI",
                )


                    
            for sql in decision.get("alter_sql", []):
                if target_table not in sql:
                    st.error("❌ Unsafe ALTER detected. Execution blocked.")
                    st.stop()
                cursor.execute(sql)

            insert_sql = decision["insert_sql"]
            dialect = st.session_state["db_dialect"]

            # ---------------------------
            # 2. Extract INSERT columns
            # ---------------------------
            cols_part = insert_sql.split("(")[1].split(")")[0]
            raw_columns = [c.strip() for c in cols_part.split(",")]

            insert_columns = [
            c.strip("[]\"").upper() for c in raw_columns
        ]

            # ---------------------------
            # 3. Normalize column names (CRITICAL)
            # ---------------------------
            df = st.session_state["df_file"]

            df.columns = [c.strip().upper() for c in df.columns]
            # insert_columns = [c.strip().upper() for c in insert_columns]

            # ---------------------------
            # 4. STRICT alignment (NO reindex)
            # ---------------------------
            df_aligned = df[insert_columns]

            placeholder = "?" if dialect == "sqlserver" else "%s"
            if insert_sql.count(placeholder) != len(insert_columns):
                insert_sql = rebuild_insert_sql(
                    target_table, insert_columns, dialect
                )

            insert_data(conn, insert_sql, df_aligned, dialect)
            log_db_change(
                db_type=dialect,
                database=database,
                schema=schema,
                table_name=target_table,
                operation="INSERT_ROWS",
                row_count=len(df),
                message="Data ingestion completed",
            )


            st.success(f"✅ {len(df_aligned)} rows ingested successfully")

        except Exception as e:
            st.error(f"❌ Execution failed: {e}")

# =========================
# SHOW PII RESULTS
# =========================
# =========================
# FINAL PII REPORT
# =========================
if (
    st.session_state.get("ingestion_mode") == "Detect PII from Existing Table"
    and "pii_scan_results" in st.session_state
):
    st.subheader("🚨 PII Scan Results (All Tables)")

    result_df = st.session_state["pii_scan_results"]

    if result_df.empty:
        st.success("✅ No PII detected in scanned tables.")
    else:
        st.dataframe(result_df)
        save_to_sqlite(
            df=result_df,
            db_path="zeusai_results.db",
            table_name="pii_scan_results"
        )

        st.metric(
            "Tables Scanned",
            result_df["table_name"].nunique()
        )

        st.metric(
            "PII Columns Detected",
            len(result_df[result_df["classification"].isin(["PII", "POSSIBLE_PII"])])
        )



# st.set_page_config(page_title="GenAI Ingestion POC", layout="wide")
# st.title("🧠 GenAI-Driven SQL Ingestion")

# # --- DB Inputs ---
# st.sidebar.header("🔌 SQL Server Connection")
# server = st.text_input("SQL Server", "123.124.0.9\\CONNECTDB")
# database = st.text_input("Database", "PowerBI")
# user = st.text_input("User")
# password = st.text_input("Password", type="password")
# driver = st.text_input("ODBC Driver", "ODBC Driver 17 for SQL Server")

# if st.button("Connect"):
#     conn = get_sqlserver_connection(server, database, user, password, driver)
#     st.session_state["conn"] = conn
#     tables_df = get_tables(conn)
#     st.session_state["tables"] = tables_df
#     st.success("Connected successfully")

# # --- Table Selection ---
# if "tables" in st.session_state:
#     tables_df = st.session_state["tables"]
#     tables_df["full"] = tables_df["TABLE_SCHEMA"] + "." + tables_df["TABLE_NAME"]

#     selected = st.selectbox("Select Target Table", tables_df["full"])
#     schema, table = selected.split(".")
#     st.session_state["selected_table"] = selected

# # --- File Upload ---
# file = st.file_uploader("Upload CSV / Excel", type=["csv", "xlsx"])

# if file and selected:
#     conn = st.session_state["conn"]

#     df_file, file_schema = get_file_schema(file)
#     db_schema = get_table_schema(conn, schema, table)

#     st.subheader("📊 Schema Comparison")
#     col1, col2 = st.columns(2)

#     with col1:
#         st.write("DB Table Schema")
#         st.dataframe(db_schema)

#     with col2:
#         st.write("Uploaded File Schema")
#         st.dataframe(file_schema)

#     st.metric("DB Columns", len(db_schema))
#     st.metric("File Columns", len(file_schema))

#     if st.button("Ask GenAI"):
#         decision = get_ingestion_decision(openai_client, db_schema, file_schema, st.session_state["selected_table"])
#         st.subheader("🤖 GenAI Decision")
#         st.code(decision, language="json")
#         st.session_state["genai_decision"] = decision
















































