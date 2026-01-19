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

def classify_impacted_views_llm(openai_client, views_df, base_table):
    payload = views_df.to_dict(orient="records")

    prompt = f"""
You are a senior data engineer performing downstream impact analysis.

Base table:
{base_table}

Classify EACH view using ONLY these rules:

1. If "SELECT * FROM {base_table}" → HIGH, precedence 1
2. If FULL JOIN or FULL OUTER JOIN → HIGH, precedence 2
3. If INNER JOIN → MODERATE_HIGH, precedence 3
4. If LEFT JOIN → LOW, precedence 4
5. If RIGHT JOIN → LOW, precedence 5

Precedence resolution:
- 1 → Definitely HIGH risk
- 2 or 3 → Moderately HIGH risk
- 4 or 5 → NOT an issue

Return ONLY valid JSON in this format:

[
  {{
    "view_name": "",
    "risk_level": "HIGH | MODERATE_HIGH | LOW",
    "reason": ""
  }}
]

Views:
{payload}
"""

    response = openai_client.responses.create(
        model="gpt-4o-mini",
        input=prompt
    )

    return response.output_text

def get_mongo_ingestion_decision(
    openai_client,
    collection_name,
    file_schema,
    existing_fields,
    ingestion_mode
):
    """
    ingestion_mode:
      - NEW_COLLECTION
      - EXISTING_COLLECTION
    """

    prompt = f"""
You are a senior data engineer working with MongoDB.

TARGET DATABASE TYPE: MongoDB
TARGET COLLECTION: {collection_name}

INGESTION MODE:
- {ingestion_mode}

STRICT RULES (DO NOT VIOLATE):
- NEVER generate SQL
- NEVER generate CREATE TABLE or ALTER TABLE
- MongoDB uses collections and documents
- Collections may be created only if explicitly approved
- New fields are allowed without schema changes

INPUT FILE FIELDS:
{file_schema['COLUMN_NAME'].tolist()}

EXISTING COLLECTION FIELDS (empty if new collection):
{existing_fields}

DECISION GUIDELINES:
- If ingestion_mode is NEW_COLLECTION, creation must be approved explicitly
- If ingestion_mode is EXISTING_COLLECTION, collection must already exist
- If a natural business key exists (id, code, email, uuid), suggest UPSERT
- If no reliable key exists, suggest INSERT
- If '_id' field exists in input, warn about conflicts
- Metadata fields may be added if useful

YOU MUST RETURN ONLY VALID JSON.
DO NOT include explanations or markdown.

JSON FORMAT (FOLLOW EXACTLY):

{{
  "action": "CREATE_AND_INSERT | DIRECT_INSERT | UPSERT",
  "create_collection": true | false,
  "insert_mode": "insert_many | upsert_many",
  "natural_key": [],
  "add_metadata": true | false,
  "metadata_fields": ["_ingested_at"],
  "warnings": []
}}
"""

    response = openai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    return response.choices[0].message.content

