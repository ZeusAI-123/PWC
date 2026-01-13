from openai import OpenAI
import streamlit as st

api_key = st.secrets["OPENAI_API_KEY"]

openai_client = OpenAI(
    api_key=api_key,
    timeout=30,
    max_retries=3  
)

def get_ingestion_decision(
    openai_client,
    db_schema,
    file_schema,
    table_name,
    db_dialect,
    ingestion_mode
):

  prompt = f"""
You are a senior data engineer.

TARGET DATABASE DIALECT: {db_dialect}

USER INGESTION MODE:
- EXISTING_TABLE → operate ONLY with ALTER + INSERT
- NEW_TABLE → MUST generate CREATE TABLE + INSERT

STRICT RULES:
- ALL columns must be VARCHAR
- NEVER modify or drop existing columns
- Operate ONLY on table: {table_name}

TABLE EXISTENCE RULE:
- If ingestion mode is NEW_TABLE, assume table does NOT exist
- If ingestion mode is EXISTING_TABLE, assume table exists

DIALECT RULES:
- sqlserver:
  - Use [COLUMN_NAME]
  - VARCHAR(255)
  - CREATE TABLE allowed
  - INSERT VALUES (?, ?, ...)
- snowflake:
  - Use "COLUMN_NAME"
  - VARCHAR
  - ALTER TABLE ADD COLUMN
  - INSERT VALUES (%s, %s, ...)

SECURITY RULES:
- Do NOT generate DROP, TRUNCATE, DELETE, UPDATE
- Do NOT reference any table other than {table_name}

INSERT RULES:
- INSERT must explicitly list column names
- Placeholder count MUST match column count

Existing table columns:
{db_schema['COLUMN_NAME'].tolist()}

Incoming file columns:
{file_schema['COLUMN_NAME'].tolist()}

Return ONLY valid JSON.

JSON FORMAT:

{{
  "action": "CREATE_AND_INSERT | ALTER_AND_INSERT | DIRECT_INSERT",
  "create_sql": [],
  "new_columns": [],
  "alter_sql": [],
  "insert_sql": ""
}}
"""




  response = openai_client.responses.create(
    model="gpt-4o-mini",
    input=prompt
)

  return response.output_text

def classify_view_risk_llm(openai_client, views_df, table_name):
    views_payload = views_df.to_dict(orient="records")

    prompt = f"""
You are a senior data engineer performing downstream impact analysis.

Target base table:
{table_name}

For EACH view below, analyze ONLY the SQL provided.

Classify risk using these EXACT rules:

1. If "SELECT * FROM {table_name}" is used → HIGH (precedence 1)
2. If FULL JOIN or FULL OUTER JOIN is used → HIGH (precedence 2)
3. If INNER JOIN is used → MODERATE_HIGH (precedence 3)
4. If LEFT JOIN is used → LOW (precedence 4)
5. If RIGHT JOIN is used → LOW (precedence 5)

Final precedence:
- Rule 1 → HIGH
- Rule 2 or 3 → MODERATE_HIGH
- Rule 4 or 5 → LOW

Return ONLY valid JSON.

JSON format:
[
  {{
    "view_name": "",
    "risk_level": "HIGH | MODERATE_HIGH | LOW",
    "precedence": 1,
    "reason": ""
  }}
]

Views:
{views_payload}
"""

    response = openai_client.responses.create(
        model="gpt-4o-mini",
        input=prompt
    )

    return response.output_text
