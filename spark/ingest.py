import pandas as pd

def insert_data(conn, insert_sql, df_aligned, dialect):
    """
    df_aligned: DataFrame already aligned to INSERT columns (order matters)
    """

    if df_aligned.empty:
        raise ValueError("❌ No rows to insert (DataFrame is empty)")

    # Convert NaN to None for DB insertion
    df_aligned = df_aligned.where(pd.notnull(df_aligned), None)

    rows = df_aligned.values.tolist()

    if not rows:
        raise ValueError("❌ No rows to insert after preprocessing")

    cursor = conn.cursor()

    if dialect == "sqlserver":
        cursor.fast_executemany = True
        cursor.executemany(insert_sql, rows)
        conn.commit()

    try:
        cursor.executemany(insert_sql, rows)
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    cursor.close()

