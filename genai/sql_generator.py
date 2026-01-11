from openai import OpenAI

# client = OpenAI()

def get_ingestion_decision(client, db_schema, file_schema, table_name, db_dialect):
    prompt = f"""
You are a senior data engineer.

TARGET DATABASE DIALECT: {db_dialect}

STRICT RULES:
- ALL columns are VARCHAR
- New columns must be VARCHAR
- Do NOT modify existing columns
- Operate ONLY on table: {table_name}

DIALECT-SPECIFIC RULES:
- If dialect is sqlserver:
  - Use [column_name] for identifiers
  - Use VARCHAR(255) for new columns
  - Do NOT use ADD COLUMN
  - INSERT must use VALUES (?, ?, ...)
- If dialect is snowflake:
  - Use "COLUMN_NAME" for identifiers
  - Use VARCHAR for new columns
  - ALTER TABLE must use ADD COLUMN
  - INSERT must use VALUES (%s, %s, ...)
- INSERT must explicitly list column names
- Number of placeholders must EXACTLY equal number of columns

PRIVILEGE RULES:
- Assume limited privileges
- Only generate ALTER TABLE ADD COLUMN statements
- Do NOT generate DROP, TRUNCATE, UPDATE, or DELETE

Existing table columns:
{db_schema['COLUMN_NAME'].tolist()}

Incoming file columns:
{file_schema['COLUMN_NAME'].tolist()}

Return ONLY valid JSON (no markdown, no explanation).

Output format (FOLLOW EXACTLY):

{{
  "action": "DIRECT_INSERT | ALTER_AND_INSERT",
  "new_columns": ["colA"],
  "alter_sql": [
    "ALTER TABLE {table_name} ADD colA VARCHAR"
  ],
  "insert_sql": "INSERT INTO {table_name} (COL1, COL2, COL3, colA) VALUES (?, ?, ?, ?)"
}}
"""



    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    return response.choices[0].message.content
