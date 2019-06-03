import praw
from config import keys, constants
from interface.interfacefactory import DataIngestionInterface
from utils import dbutils, logutils
from reddit import redditutils


class RedditDataIngestion(DataIngestionInterface):

    def __init__(self,client_id=keys.reddit['client_id'],client_secret=keys.reddit['client_secret'],user_agent=keys.reddit['user_agent']):
        self.reddit_agent = praw.Reddit(client_id=client_id,client_secret=client_secret,user_agent=user_agent)
        self.logger = logutils.get_logger('Reddit Data Ingestion')

    def data_config(self, uri, **kwargs):
        pass

    def get_data(self, query, **kwargs):
        search_result_list = list([])
        subreddit_name = 'all'
        if 'subreddit_name' in kwargs.keys():
            subreddit_name = kwargs['subreddit_name']
        results = self.reddit_agent.subreddit(subreddit_name).search(query)
        for search_result in results:
            search_result_list.append(search_result)
        return search_result_list

    def parse_data(self, data_list,product='default'):
        parsed_subreddit_data_list = list([])
        for subreddit in data_list: #TODO - try-catch here
            try:
                subreddit_data = dict({'ups': subreddit.ups, 'downs': subreddit.downs, 'title': subreddit.title,
                                       'created': subreddit.created, 'author': subreddit.author.name, 'product': product,
                                       'created_human_date': redditutils.get_python_date_from_unix_timestamp(subreddit.created)})
                parsed_subreddit_data_list.append(subreddit_data)
            except:
                self.logger.error("Error while parsing data for product %s " % product)
        return parsed_subreddit_data_list

    def store_data(self, data_list, connection_object):
        connection_object.set_collection(constants.REDDIT_COLLECTION_NAME)
        for data_dict in data_list: # TODO - try-catch here
            try:
                if redditutils.check_duplicate_post(data_dict) is False:
                    connection_object.insert_document(data_dict)
            except:
                self.logger.error("Error while inserting data")
        connection_object.close_connection()

    def data_ingestion_pipeline(self, query,product='default'):
        search_data_list = self.get_data(query)
        self.logger.info("Data Fetched for product %s " % product)
        parsed_data_list = self.parse_data(search_data_list,product=product)
        self.logger.info("Data Parsed for product %s " % product)
        mongo_connector = dbutils.get_mongodb_connection()
        self.store_data(parsed_data_list, mongo_connector)
        self.logger.info("Data Stored for product %s " % product)
