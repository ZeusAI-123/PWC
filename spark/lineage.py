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
    
def get_view_definitions(conn, database, schema, view_names):
    views_list = ",".join([f"'{v}'" for v in view_names])

    query = f"""
    SELECT
        table_schema,
        table_name AS view_name,
        view_definition
    FROM {database}.INFORMATION_SCHEMA.VIEWS
    WHERE table_schema = '{schema}'
      AND table_name IN ({views_list})
    """

    return pd.read_sql(query, conn)
