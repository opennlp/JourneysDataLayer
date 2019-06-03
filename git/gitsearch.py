from utils import logutils, dbutils
from config import constants, keys
from git import gitsearchutils
from interface.interfacefactory import DataIngestionInterface
import requests

BASE_URL = 'https://git.corp.adobe.com/api/v3/search/issues'


class GitDataIngestion(DataIngestionInterface):
    def __init__(self, access_token=keys.git['access_token']):
        self.ACCESS_TOKEN = access_token
        self.logger = logutils.get_logger("Git Data Ingestion")

    def data_config(self, search_query, **kwargs):
        uri = BASE_URL + '?' + 'q=' + search_query
        uri = uri + '&' + 'access_token=' + self.ACCESS_TOKEN
        return uri

    def get_data(self, uri, **kwargs):
        response = requests.get(uri)
        if response.status_code == 200:
            return response.json()
        return "Error"

    def parse_data(self, data_list, product='photoshop'):
        item_list = data_list['items']
        parsed_data_list = []
        for item in item_list:
            try:
                parsed_data_list.append({'product': product,
                                         'url': item['url'],
                                         'id': item['id'],
                                         'title': item['title'],
                                         'state': item['state'],
                                         'comments': item['comments'],
                                         'updated_at': gitsearchutils.convert_string_timestamp_to_python_date(item['updated_at']),
                                         'body': item['body']})
            except Exception:
                self.logger.error("Error occurred while inserting item %s " % item['url'])
        return parsed_data_list

    def store_data(self, data_list, connection_object):
        connection_object.set_collection(constants.GIT_COLLECTION_NAME)
        for data_dict in data_list:
            try:
                if gitsearchutils.check_duplicate_document(data_dict) is False:
                    connection_object.insert_document(data_dict)
            except Exception:
                self.logger.error("Error occurred while inserting data %s" % data_dict['url'])
        connection_object.close_connection()

    def data_ingestion_pipeline(self, search_query, product):
        uri = self.data_config(search_query)
        self.logger.info("Data Source Configured for product %s " % product)
        response_data = self.get_data(uri)
        self.logger.info("Data fetched for product %s " % product)
        parsed_data_list = self.parse_data(response_data, product=product)
        self.logger.info("Data parsed for product %s " % product)
        connection_object = dbutils.get_mongodb_connection()
        self.store_data(parsed_data_list, connection_object)
        self.logger.info("Data stored for product %s " % product)
