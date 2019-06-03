from datetime import datetime
from config import constants
from utils import dbutils


def get_python_date_from_unix_timestamp(date_integer):
    return datetime.utcfromtimestamp(int(date_integer)).strftime('%Y-%m-%d %H:%M:%S')


def check_duplicate_document(document):
    mongo_connector = dbutils.get_mongodb_connection()
    mongo_connector.set_collection(constants.DAILYMOTION_COLLECTION_NAME)
    query = dict({'id': document['id'], 'product': document['product']})
    if mongo_connector.check_document(query) is False:
        mongo_connector.close_connection()
        return False
    mongo_connector.close_connection()
    return True
