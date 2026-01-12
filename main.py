import streamlit as st
import pandas as pd
from datasources.sqlserver import get_sqlserver_connection
from datasources.snowflake import connect_snowflake
from dotenv import load_dotenv
import os
from spark.schema_compare import get_tables, get_table_schema
from genai.sql_generator import get_ingestion_decision
from spark.schema_compare import get_file_schema
from spark.ingest import insert_data
from openai import OpenAI
import json
# Load environment variables first
st.session_state.setdefault("ingestion_mode", None)
st.session_state.setdefault("selected_table", None)
st.session_state.setdefault("decision", None)
st.session_state.setdefault("confirm", False)
load_dotenv()


# api_key = st.secrets["OPENAI_API_KEY"]

# openai_client = OpenAI(
#     api_key=api_key,
#     timeout=30,
#     max_retries=3  
# )

st.set_page_config(
    page_title="ZeusAI SQL Ingestion",
    layout="wide"
)

st.title("🧠 ZeusAI-Driven DATA Ingestion")

def safe_json_loads(text):
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Attempt to extract JSON from text
        start = text.find("{")
        end = text.rfind("}") + 1

        if start != -1 and end != -1:
            return json.loads(text[start:end])

        raise ValueError("❌ GenAI did not return valid JSON")
    
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

db_type = st.radio(
    "Select Database Type",
    ["SQL Server", "Snowflake"]
)

if db_type == "SQL Server":
    st.subheader("🔌 SQL Server Connection")

    server = st.text_input("Server")
    database = st.text_input("Database")
    user = st.text_input("User")
    password = st.text_input("Password", type="password")
    driver = st.text_input(
        "ODBC Driver",
        "ODBC Driver 17 for SQL Server"
    )

if db_type == "Snowflake":
    st.subheader("❄️ Snowflake Connection")

    account = st.text_input("Account (without .snowflakecomputing.com)")
    user = st.text_input("User")
    password = st.text_input("Password", type="password")
    warehouse = st.text_input("Warehouse")
    database = st.text_input("Database")
    schema = st.text_input("Schema")
    role = st.text_input("Role (optional)")


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
    st.subheader("🧭 Ingestion Mode")

    ingestion_mode = st.radio(
        "Choose ingestion type",
        ["Ingest into Existing Table", "Create New Table (GenAI)"],
        index=None
    )

    st.session_state["ingestion_mode"] = ingestion_mode
# =========================
# 3. TABLE SELECTION
# =========================
if st.session_state["ingestion_mode"] and "tables" in st.session_state:
    # st.subheader("📋 Select Table")

    tables_df = st.session_state["tables"]

    # 🔒 Always ensure full_name exists
    # if "full_name" not in tables_df.columns:
    #     tables_df["full_name"] = (
    #         tables_df["TABLE_SCHEMA"] + "." + tables_df["TABLE_NAME"]
    #     )
    #     st.session_state["tables"] = tables_df  # update session state

    if st.session_state["ingestion_mode"] == "Ingest into Existing Table":
        selected_table = st.selectbox(
            "Choose existing table",
            tables_df["full_name"]
        )
    else:
        selected_table = st.text_input(
            "Enter new table name",
            placeholder="schema.new_table"
        )
        if not selected_table:
            st.info("ℹ️ Enter a table name to continue")
            st.stop()

    st.session_state["selected_table"] = selected_table

    dialect = st.session_state["db_dialect"]

    if dialect == "sqlserver":
        schema_name, table_name = selected_table.split(".")
    else:
        schema_name = None
        table_name = selected_table


# =========================
# 4. FILE UPLOAD
# =========================
if st.session_state.get("selected_table"):
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
        if st.session_state["ingestion_mode"] == "Create New Table (GenAI)":
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

    # =========================
    # 6. GENAI DECISION
    # =========================
    if st.button("🤖 Ask Zeus"):
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

    if decision["alter_sql"]:
        st.markdown("### 🛠 ALTER TABLE SQL")
        for sql in decision["alter_sql"]:
            st.code(sql, language="sql")

    st.markdown("### 📥 INSERT SQL")
    st.code(decision["insert_sql"], language="sql")

    # =========================
    # 8. USER CONFIRMATION
    # =========================
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
                for sql in decision["create_sql"]:
                    if target_table.lower() not in sql.lower():
                        st.error("❌ Unsafe CREATE detected")
                        st.stop()
                    cursor.execute(sql)
                    
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















