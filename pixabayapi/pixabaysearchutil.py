from utils import dbutils
from config import constants


def check_duplicate_document(document):
    mongo_connector = dbutils.get_mongodb_connection()
    mongo_connector.set_collection(constants.PIXABAY_COLLECTION_NAME)
    query = dict({'id': document['id'], 'product': document['product']})
    if mongo_connector.check_document(query) is False:
        mongo_connector.close_connection()
        return False
    mongo_connector.close_connection()
    return True
