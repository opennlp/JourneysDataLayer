from datetime import datetime
from utils import dbutils
from config import constants


def get_tweet_source_name(source_string):
    source_name = ''
    start_index = source_string.find('>')
    if start_index != -1:
        end_index = source_string.find('<', start_index+1)
        if end_index != -1:
            source_name = source_string[start_index+1:end_index]
    return source_name


def get_python_date_from_tweet(tweet_date_string,format='%a %b %d %H:%M:%S +0000 %Y'):
    tweet_date = datetime.strptime(tweet_date_string,format)
    return tweet_date


def check_for_duplicate(document):
    mongo_connector = dbutils.get_mongodb_connection()
    mongo_connector.set_collection(constants.TWITTER_COLLECTION_NAME)
    query = dict({'product': document['product'], 'id': document['id']})
    cursor = mongo_connector.find_document(query)
    if cursor.count() > 0:
        mongo_connector.close_connection()
        return True
    mongo_connector.close_connection()
    return False
