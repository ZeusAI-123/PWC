# from pymongo import MongoClient
# from pymongo.errors import CollectionInvalid

# def get_mongodb_connection(
#     host,
#     port,
#     database,
#     username,
#     password
# ):
#     client = MongoClient(
#         host=host,
#         port=port,
#         username=username,
#         password=password,
#         authSource=database
#     )

#     return client[database]
from pymongo import MongoClient

def get_mongodb_connection(uri: str):
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")  # fail fast if unreachable
    return client.get_default_database()
