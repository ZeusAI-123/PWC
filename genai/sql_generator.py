from openai import OpenAI

# client = OpenAI()

def get_ingestion_decision(
    client,
    db_schema,
    file_schema,
    table_name,
    db_dialect,
    ingestion_mode
):

#     prompt = f"""
# You are a senior data engineer.

# TARGET DATABASE DIALECT: {db_dialect}

# STRICT RULES:
# - ALL columns must be VARCHAR
# - NEVER modify or drop existing columns
# - Operate ONLY on table: {table_name}

# TABLE EXISTENCE RULE:
# - If existing table columns list is EMPTY, the table DOES NOT exist
# - In that case, you MUST generate CREATE TABLE

# DIALECT RULES:
# - sqlserver:
#   - Use [COLUMN_NAME]
#   - VARCHAR(255)
#   - CREATE TABLE allowed
#   - INSERT VALUES (?, ?, ...)
# - snowflake:
#   - Use "COLUMN_NAME"
#   - VARCHAR
#   - ALTER TABLE ADD COLUMN
#   - INSERT VALUES (%s, %s, ...)

# SECURITY RULES:
# - Do NOT generate DROP, TRUNCATE, DELETE, UPDATE
# - Do NOT reference any table other than {table_name}

# INSERT RULES:
# - INSERT must explicitly list column names
# - Placeholder count MUST match column count

# Existing table columns:
# {db_schema['COLUMN_NAME'].tolist()}

# Incoming file columns:
# {file_schema['COLUMN_NAME'].tolist()}

# Return ONLY valid JSON.

# JSON FORMAT (FOLLOW EXACTLY):

# {{
#   "action": "CREATE_AND_INSERT | ALTER_AND_INSERT | DIRECT_INSERT",
#   "create_sql": [
#     "CREATE TABLE {table_name} (COL1 VARCHAR, COL2 VARCHAR)"
#   ],
#   "new_columns": ["COL3"],
#   "alter_sql": [
#     "ALTER TABLE {table_name} ADD COLUMN COL3 VARCHAR"
#   ],
#   "insert_sql": "INSERT INTO {table_name} (COL1, COL2, COL3) VALUES (%s, %s, %s)"
# }}
# """
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




  response = client.responses.create(
    model="gpt-4o-mini",
    input=prompt
)

  return response.choices[0].message.content

