import pandas as pd

def get_impacted_views_snowflake(conn, database, schema, table):
    query = f"""
    SELECT
        referencing_schema,
        referencing_object_name AS view_name,
        referencing_object_domain AS object_type
    FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
    WHERE referenced_database = '{database}'
      AND referenced_schema   = '{schema}'
      AND referenced_object_name = '{table}'
      AND referencing_object_domain IN ('VIEW', 'MATERIALIZED VIEW')
    """
    return pd.read_sql(query, conn)
