from config import constants
from utils import dbutils


def check_duplicate_document(document):
    mongo_connector = dbutils.get_mongodb_connection()
    mongo_connector.set_collection(constants.LIBRARYIO_COLLECTION_NAME)
    query = dict({'Product': document['Product'], 'Latest Release': document['Latest Release']})
    if mongo_connector.check_document(query) is False:
        mongo_connector.close_connection()
        return False
    mongo_connector.close_connection()
    return True
