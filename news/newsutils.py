from datetime import datetime
from utils import dbutils
from config import constants


def convert_string_timestamp_to_python_date(string_date_utc):
    string_date = string_date_utc.strip()
    string_date = string_date.replace('T', ' ')
    string_date = string_date.replace('Z', '')
    return datetime.strptime(string_date, "%Y-%m-%d %H:%M:%S")


def check_duplicate_document(document):
    mongo_connector = dbutils.get_mongodb_connection()
    mongo_connector.set_collection(constants.NEWS_COLLECTION_NAME)
    query = dict({'url': document['url']})
    if mongo_connector.check_document(query) is False:
        mongo_connector.close_connection()
        return False
    mongo_connector.close_connection()
    return True
