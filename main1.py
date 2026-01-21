import streamlit as st
import pandas as pd
from datasources.sqlserver import get_sqlserver_connection
from datasources.snowflake import connect_snowflake
from dotenv import load_dotenv
import os
from spark.schema_compare import get_tables, get_table_schema
from genai.sql_generator import get_ingestion_decision, classify_impacted_views_llm
from spark.schema_compare import get_file_schema
from spark.lineage import get_impacted_views_snowflake, get_view_definitions
from spark.ingest import insert_data
from openai import OpenAI
import re
import json
import time

# Load environment variables first
st.session_state.setdefault("ingestion_mode", None)
st.session_state.setdefault("selected_table", None)
st.session_state.setdefault("decision", None)
st.session_state.setdefault("confirm", False)
schema_name = None
table_name = None
dialect = None
load_dotenv()


api_key = st.secrets["OPENAI_API_KEY"]

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

        if dialect == "sqlserver":
            tables_df["full_name"] = (
                tables_df["TABLE_SCHEMA"] + "." + tables_df["TABLE_NAME"]
            )
        else:
            tables_df["full_name"] = tables_df["TABLE_NAME"]

        st.session_state["tables"] = tables_df
        st.success(f"✅ Connected to {db_type}")

    except Exception as e:
        st.error(f"❌ Connection failed: {e}")
        st.stop()
# =========================
# 3. INGESTION MODE
# =========================
if "conn" in st.session_state:
    st.subheader("🧭 Select Operation")

    ingestion_mode = st.radio(
        "Choose ingestion type",
        [
            "Ingest into Existing Table",
            "Create New Table & Ingest",
            "Detect PII from Existing Table"
        ],
        index=None
    )

    st.session_state["ingestion_mode"] = ingestion_mode
# =========================
# 3. TABLE SELECTION
# =========================
# =========================
# PII DETECTION — ALL TABLES
# =========================
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

if st.session_state["ingestion_mode"] and "tables" in st.session_state:
    
    
        # st.subheader("📋 Select Table")
    
        tables_df = st.session_state["tables"]
    
        # 🔒 Always ensure full_name exists
        # if "full_name" not in tables_df.columns:
        #     tables_df["full_name"] = (
        #         tables_df["TABLE_SCHEMA"] + "." + tables_df["TABLE_NAME"]
        #     )
        #     st.session_state["tables"] = tables_df  # update session state
    
        if st.session_state.get("ingestion_mode") == "Ingest into Existing Table":
    
            selected_table = st.selectbox(
                "Choose existing table",
                tables_df["full_name"]
            )
            # st.session_state["selected_table"] = selected_table
        elif st.session_state.get("ingestion_mode") == "Create New Table & Ingest":
            selected_table = st.text_input(
                "Enter new table name",
                placeholder="schema.new_table"
            )
            if not selected_table:
                st.info("ℹ️ Enter a table name to continue")
                st.stop()
    
        if "selected_table" in locals():
            st.session_state["selected_table"] = selected_table

    
        dialect = st.session_state["db_dialect"]
    
        if "selected_table" in locals():
            if dialect == "sqlserver":
                schema_name, table_name = selected_table.split(".")
            else:
                schema_name = None
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
                schema_name,
                table_name,
                dialect
            )
            db_schema["DATA_TYPE"] = "varchar"
    
    # =========================
    # 5. SHOW SCHEMA DETAILS
    # =========================
        st.subheader("📊 Data Catalog")

        if st.session_state["ingestion_mode"] == "Create New Table (GenAI)":
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









































