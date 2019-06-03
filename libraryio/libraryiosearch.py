import requests
from interface.interfacefactory import DataIngestionInterface
from config import keys, constants
from utils import dbutils, logutils
from libraryio import libraryiosearchutil


class LibraryIODataIngestion(DataIngestionInterface):
    def __init__(self, api_key=keys.library_io['api_key']):
        self.api_key = api_key
        self.logger = logutils.get_logger("Library IO Data Ingestion")

    def data_config(self, search_term, **kwargs):
        base_url = 'https://libraries.io/api/search?q='
        api_key_url = '&api_key='
        url = base_url + search_term + api_key_url + self.api_key
        return url

    def get_data(self, uri, **kwargs):
        response = requests.get(uri)
        return response.json()

    def parse_data(self, data_list, product='photoshop'):
        parsed_response_list = list([])
        for response_json in data_list:
            response_dict = dict({'Name': response_json['name'], 'Platform': response_json['platform'],
                                  'Description': response_json['description'], 'Rank': response_json['rank'],
                                  'Latest Release': response_json['latest_release_published_at'],
                                  'Language': response_json['language'], 'Stars': response_json['stars'],
                                  'Forks': response_json['forks'],
                                  'Dependents Count': response_json['dependents_count'],
                                  'Dependent Repos Count': response_json['dependent_repos_count'],
                                  'Keywords': "| ".join(response_json['keywords']), 'Product': product})
            parsed_response_list.append(response_dict)

        return parsed_response_list

    def store_data(self, data_list, connection_object):
        connection_object.set_collection(constants.LIBRARYIO_COLLECTION_NAME)
        for data_dict in data_list:
            try:
                if libraryiosearchutil.check_duplicate_document(data_dict) is False:
                    connection_object.insert_document(data_dict)
            except Exception:
                self.logger.error("Error occurred while inserting data %s " % data_dict['Product'])
        connection_object.close_connection()

    def data_ingestion_pipeline(self, search_term, product_name):
        uri = self.data_config(search_term)
        self.logger.info("Data Source Configured for product %s " % product_name)
        response_data = self.get_data(uri)
        self.logger.info("Data Fetched for product %s " % product_name)
        parsed_data_list = self.parse_data(response_data, product=product_name)
        self.logger.info("Data Parsed for product for %s " % product_name)
        mongo_connector = dbutils.get_mongodb_connection()
        self.store_data(parsed_data_list, mongo_connector)
        self.logger.info("Data stored for product %s " % product_name)
