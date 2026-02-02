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

def get_impacted_objects_sqlserver(conn, schema, table):

    query = """
    SELECT
        sch.name AS base_schema,
        obj.name AS base_object,

        sch2.name AS dependent_schema,
        obj2.name AS dependent_object,
        obj2.type_desc AS dependent_type

    FROM sys.sql_expression_dependencies d

    JOIN sys.objects obj
        ON d.referenced_id = obj.object_id
    JOIN sys.schemas sch
        ON obj.schema_id = sch.schema_id

    JOIN sys.objects obj2
        ON d.referencing_id = obj2.object_id
    JOIN sys.schemas sch2
        ON obj2.schema_id = sch2.schema_id

    WHERE sch.name = ?
      AND obj.name = ?
    """

    return pd.read_sql(query, conn, params=[schema, table])
