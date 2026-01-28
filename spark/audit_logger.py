import sqlite3
import json


DB_PATH = "catalog_audit.db"


def init_app_action_log():

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS app_action_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_type TEXT,
        object_name TEXT,
        object_type TEXT,
        message TEXT,
        details_json TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    conn.commit()
    conn.close()


def init_db_change_log():

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS db_change_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        db_type TEXT,
        database_name TEXT,
        schema_name TEXT,
        table_name TEXT,
        operation TEXT,
        row_count INTEGER,
        message TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    conn.commit()
    conn.close()


def log_app_action(
    event_type,
    object_name=None,
    object_type=None,
    message=None,
    details=None,
):

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO app_action_log
        (
            event_type,
            object_name,
            object_type,
            message,
            details_json
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            event_type,
            object_name,
            object_type,
            message,
            json.dumps(details) if details else None,
        ),
    )

    conn.commit()
    conn.close()


def log_db_change(
    db_type,
    database,
    schema,
    table_name,
    operation,
    row_count=None,
    message=None,
):

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO db_change_log
        (
            db_type,
            database_name,
            schema_name,
            table_name,
            operation,
            row_count,
            message
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            db_type,
            database,
            schema,
            table_name,
            operation,
            row_count,
            message,
        ),
    )

    conn.commit()
    conn.close()
