import pandas as pd

def get_tables(conn, dialect, schema=None):
    if dialect == "sqlserver":

        query = """
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        """

        params = None

        if schema:
            query += " AND TABLE_SCHEMA = ?"
            params = [schema]

        return pd.read_sql(query, conn, params=params)

    elif dialect == "snowflake":

        query = """
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        """

        params = None

        if schema:
            query += " AND TABLE_SCHEMA = %s"
            params = [schema]

        return pd.read_sql(query, conn, params=params)



def get_table_schema(conn, schema, table, dialect):
    if dialect == "sqlserver":
        query = f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}'
          AND TABLE_NAME = '{table}'
        """
    else:  # snowflake
        query = f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table}'
        """

    return pd.read_sql(query, conn)


def get_file_schema(file):

    if file.name.endswith(".csv"):
        df = pd.read_csv(file, dtype=str)
    else:
        df = pd.read_excel(file, dtype=str)

    # Force all values to string
    df = df.astype(str)

    schema = pd.DataFrame({
        "COLUMN_NAME": df.columns,
        "DATA_TYPE": ["varchar"] * len(df.columns)
    })

    return df, schema
