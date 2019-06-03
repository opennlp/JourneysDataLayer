from utils import dbutils
from config import constants


def check_duplicate(document):
    mongo_connector = dbutils.get_mongodb_connection()
    mongo_connector.set_collection(constants.IMGUR_COLLECTION_NAME)
    query = dict({'product': document['product'], 'source': document['source']})
    if mongo_connector.check_document(query) is False:
        mongo_connector.close_connection()
        return False
    mongo_connector.close_connection()
    return True
