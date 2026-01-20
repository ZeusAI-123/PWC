from pymongo import MongoClient
from pymongo.errors import CollectionInvalid

def get_mongodb_connection(
    host,
    port,
    database,
    username,
    password
):
    client = MongoClient(
        host=host,
        port=port,
        username=username,
        password=password,
        authSource=database
    )

    return client[database]

