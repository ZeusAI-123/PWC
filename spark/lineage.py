import pandas as pd

def get_impacted_views(conn, database, schema, table):
    query = f"""
    SELECT
        referencing_object_schema AS view_schema,
        referencing_object_name   AS view_name,
        referencing_object_domain AS object_type
    FROM {database}.INFORMATION_SCHEMA.OBJECT_DEPENDENCIES
    WHERE referenced_object_schema = '{schema}'
      AND referenced_object_name   = '{table}'
      AND referencing_object_domain IN ('VIEW', 'MATERIALIZED VIEW')
    """
    return pd.read_sql(query, conn)
