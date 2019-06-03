from datetime import datetime
from config import constants
from utils import dbutils


def get_python_date_from_unix_timestamp(date_integer):
    return datetime.utcfromtimestamp(int(date_integer)).strftime('%Y-%m-%d %H:%M:%S')


def check_duplicate_post(document):
    mongo_connector = dbutils.get_mongodb_connection()
    mongo_connector.set_collection(constants.REDDIT_COLLECTION_NAME)
    query = dict({'created': document['created'], 'author': document['author'], 'product': document['product']})
    if mongo_connector.check_document(query) is True:
        mongo_connector.close_connection()
        return True
    mongo_connector.close_connection()
    return False

