from config import keys,constants
from newsapi.articles import Articles
from newsapi.sources import Sources
import requests
from utils import dbutils, logutils
from news import newsutils
from interface.interfacefactory import DataIngestionInterface


class NewsDataIngestion(DataIngestionInterface):

    def __init__(self,api_key=keys.news['api_key']):
        self.api_key = api_key
        self.article = Articles(self.api_key)
        self.source = Sources(self.api_key)
        self.base_url = keys.news['base_everything_url']
        self.logger = logutils.get_logger('News Data Ingestion')

    def get_data(self, query, from_date=None, to_date=None, page_size=100, sort_by='publishedAt', language='en', **kwargs):
        key_value_params = {
            'apiKey': self.api_key,
            'q': query,
            'from': from_date,
            'to': to_date,
            'sortBy': sort_by,
            'pageSize': page_size,
            'language': language
        }

        url = self.data_config(self.base_url, query_separator='?', key_value_params=key_value_params)
        response = requests.get(url)
        return response.json()

    def data_config(self, base_url, **kwargs):
        query_separator = None
        key_value_params = None
        join_sep = '&'
        url = base_url
        if 'query_separator' in kwargs.keys():
            query_separator = kwargs['query_separator']
        if 'key_value_params' in kwargs.keys():
            key_value_params = kwargs['key_value_params']
        if query_separator is not None:
            url = base_url + str(query_separator)
        if key_value_params is not None:
            for key in key_value_params.keys():
                if key_value_params[key] is not None:
                    url = url + str(key) + '=' + str(key_value_params[key]) + join_sep
        return url[:-1]

    def store_data(self, data_list, connection_object):
        connection_object.set_collection(constants.NEWS_COLLECTION_NAME)
        for data_dict in data_list:
            try:
                if newsutils.check_duplicate_document(data_dict) is False:
                    connection_object.insert_document(data_dict)
            except:
                self.logger.error('Error while inserting data')
        connection_object.close_connection()

    def parse_data(self, news_response_json, product='default'):
        article_list = list([])
        if news_response_json['status'] == 'ok':
            article_list = news_response_json['articles']
        for article in article_list:
            try:
                article['source'] = article['source']['name']
                article['product'] = product
                article['human_date'] = newsutils.convert_string_timestamp_to_python_date(article['publishedAt'])
            except:
                self.logger.error("error while parsing data")
        return article_list

    def get_articles(self,source_id,selection_type="popular"):
        if selection_type == 'latest':
            return self.article.get_by_latest(source_id)
        elif selection_type == 'top':
            return self.article.get_by_top(source_id)
        else:
            return self.article.get_by_popular(source_id)

    def data_ingestion_pipeline(self,query,product='default'):
        news_json = self.get_data(query)
        self.logger.info("News data fetched for product %s " % product)
        parsed_news_list = self.parse_data(news_json, product=product)
        self.logger.info("News Data parsed for product %s" % product)
        mongo_connector = dbutils.get_mongodb_connection()
        self.store_data(parsed_news_list, mongo_connector)
        self.logger.info("News data stored for product %s " % product)
