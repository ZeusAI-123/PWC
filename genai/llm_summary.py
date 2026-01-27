import os
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# =======================
# PROMPT TEMPLATES
# =======================

VIEW_PROMPT = """
Act as a Technical Data Analyst.

Analyze the provided SQL VIEW definition and generate a high-density
Technical Specification Mapping.

GOAL:
Provide a developer with a concise cheat sheet of the logic,
without excessive explanation or storytelling.

IMPORTANT RULES:
- Do NOT suggest changes, optimizations, or execution
- Stick strictly to the requested format
- Be precise and technical
- Assume the reader understands SQL basics

========================
SQL VIEW NAME:
{object_name}

SQL VIEW DEFINITION:
{object_sql}
========================

STRUCTURE THE RESPONSE USING THIS EXACT FORMAT:

1. Object Metadata
View Name: {object_name}

Primary Grain:
(Describe what one row represents, e.g., one row per customer per day)

Refresh Type:
(Standard View / Materialized View – infer from SQL if possible)

2. Source Mapping & Joins

Alias | Source Table/View | Join Type | Join Condition | Purpose

3. Filtering Logic (WHERE / HAVING)
- List each filter condition exactly as used
- Explain the business rule for each condition

4. Calculated Columns Logic
List ONLY columns involving:
- CASE statements
- Mathematical calculations
- String manipulation

Format:
Column_Name: Explanation of logic

5. Performance Critical Fields
- Columns used in JOIN conditions
- Columns used in WHERE / HAVING clauses
- Mention base tables where indexing is important
"""

PROC_PROMPT = """
Act as a Senior Backend Developer.

Analyze the following SQL STORED PROCEDURE and generate a
Technical Execution Map.

GOAL:
Provide a concise technical breakdown so a developer can debug,
maintain, or modify the procedure without reading the entire code.

IMPORTANT RULES:
- Do NOT rewrite or optimize SQL
- Do NOT suggest improvements
- Focus on execution behavior and data flow
- Use bullet points and step-based explanations

========================
SQL PROCEDURE NAME:
{object_name}

SQL PROCEDURE DEFINITION:
{object_sql}
========================

STRUCTURE THE RESPONSE USING THIS EXACT FORMAT:

1. Interface (Input / Output)

Parameter | Data Type | Default | Direction | Purpose

2. Dependency List
Reads From:
- List tables/views queried

Writes To:
- List tables inserted/updated/deleted

Calls:
- Other procedures, functions, or triggers (if any)

3. Transactional Flow (Step-by-Step)
Step 1:
- Describe the first logical operation

Step 2:
- Describe the next major action

Step N:
- Continue until procedure completion

4. Logic & Transformation Rules
Filtering:
- WHERE clause conditions used in main logic

Business Rules:
- CASE expressions
- IF / ELSE branching
- Conditional data flow rules

5. Error Handling & Commit Logic
Transactions:
- Explicit (BEGIN TRAN / COMMIT / ROLLBACK) or Implicit

Error Catching:
- TRY...CATCH blocks
- RAISERROR or THROW usage
"""

# =======================
# MAIN FUNCTION
# =======================

def generate_sql_documentation(object_name, object_type, object_sql):
    """
    Generates GenAI technical documentation for SQL Views and Stored Procedure
    .
    """

    if object_type.lower() == "view":
        prompt = VIEW_PROMPT.format(
            object_name=object_name,
            object_sql=object_sql
        )

    elif object_type.lower() in ["procedure", "proc", "stored procedure"]:
        prompt = PROC_PROMPT.format(
            object_name=object_name,
            object_sql=object_sql
        )

    else:
        raise ValueError("Unsupported object type. Use 'view' or 'procedure'.")

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0.2,
        messages=[
            {"role": "system", "content": "You are an expert SQL technical documentation generator."},
            {"role": "user", "content": prompt}
        ]
    )

    return response.choices[0].message.content
