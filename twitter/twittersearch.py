from tweepy import OAuthHandler
import tweepy
from config import keys, constants
from utils import dbutils, logutils
from interface.interfacefactory import DataIngestionInterface
from twitter import twitterutils


consumer_key = keys.twitter['consumer_key']
consumer_secret = keys.twitter['consumer_secret']
access_token = keys.twitter['access_token']
access_token_secret = keys.twitter['access_token_secret']

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)


class TwitterDataIngestion(DataIngestionInterface):

    def __init__(self):
        self.logger = logutils.get_logger('Twitter Data Ingestion')

    @staticmethod
    def get_filtered_json(status_model):
        search_result_json = dict({})
        if status_model is not None:
            search_result_json['text'] = status_model.text
            search_result_json['id'] = status_model.id
            search_result_json['source'] = status_model.source
            search_result_json['retweet_count'] = status_model.retweet_count
            search_result_json['created_at'] = status_model.created_at
            search_result_json['lang'] = status_model.lang
            search_result_json['place'] = status_model.place
            search_result_json['geo'] = status_model.geo
        return search_result_json

    def store_data(self, data_list, connection_object):
        connection_object.set_collection(constants.TWITTER_COLLECTION_NAME)
        for data_dict in data_list:
            try:
                if twitterutils.check_for_duplicate(data_dict) is False:
                    connection_object.insert_document(data_dict)
            except:
                self.logger.error("Exception while inserting data")
        connection_object.close_connection()

    def get_data(self, search_term, **kwargs):
        self.api = tweepy.API(auth)
        count = 100
        lang = 'en'
        if 'count' in kwargs.keys():
            count = kwargs['count']
        if 'lang' in kwargs.keys():
            lang = kwargs['lang']
        search_result_list = self.api.search(q=search_term, count=count, lang=lang)
        return search_result_list

    def data_config(self, search_query, **kwargs):
        pass

    def parse_data(self, data_list, product='default'):
        parsed_data_list = list([])
        for data in data_list:
            try:
                filtered_data = self.get_filtered_json(data)
                filtered_data['product'] = product
                parsed_data_list.append(filtered_data)
            except:
                self.logger.error("Error while parsing data for product %s " % product)
        return parsed_data_list

    def data_ingestion_pipeline(self,query_term,product):
        data_list = self.get_data(query_term)
        self.logger.info("Data fetched for product %s" % product)
        parsed_data_list = self.parse_data(data_list, product=product)
        self.logger.info("Data parsed for product %s" % product)
        mongo_connector = dbutils.get_mongodb_connection()
        self.store_data(parsed_data_list, mongo_connector)
        self.logger.info("Data Inserted for product %s " % product)
