import snowflake.connector

def connect_snowflake(
    account,
    user,
    password,
    warehouse,
    database,
    schema,
    role=None
):
    return snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        warehouse=warehouse,
        database=database,
        schema=schema,
        role=role
    )
