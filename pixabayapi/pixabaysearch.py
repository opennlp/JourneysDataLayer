from pixabay import Image, Video
from config import keys, constants
from interface.interfacefactory import DataIngestionInterface
from utils import dbutils, logutils, dateutils
from pixabayapi import pixabaysearchutil


class PixabayDataIngestion(DataIngestionInterface):
    def __init__(self, api_key=keys.pixabay['api_key']):
        self.api_key = api_key
        self.logger = logutils.get_logger("Pixabay Data Ingestion")

    def data_config(self, uri, **kwargs):
        pass

    def get_data(self, search_query, **kwargs):
        result_type='image'
        if 'result_type' in kwargs.keys():
            result_type = kwargs['result_type']
        if result_type == 'image':
            return Image(self.api_key).search(q=search_query, per_page=20)['hits']
        else:
            return Video(self.api_key).search(q=search_query, per_page=20)['hits']

    def parse_data(self, data_list, product_name='photoshop'):
        parsed_data_list = []
        for data_dict in data_list:
            parsed_data_dict = dict({})
            parsed_data_dict['comments'] = data_dict['comments']
            parsed_data_dict['downloads'] = data_dict['downloads']
            parsed_data_dict['favorites'] = data_dict['favorites']
            parsed_data_dict['likes'] = data_dict['likes']
            parsed_data_dict['views'] = data_dict['views']
            parsed_data_dict['webformatURL'] = data_dict['webformatURL']
            parsed_data_dict['user'] = data_dict['user']
            parsed_data_dict['product'] = product_name
            parsed_data_list['inserted_date'] = dateutils.get_current_date_in_string()
            parsed_data_dict['id'] = data_dict['id']
            parsed_data_list.append(parsed_data_dict)
        return parsed_data_list

    def store_data(self, data_list, connection_object):
        connection_object.set_collection(constants.PIXABAY_COLLECTION_NAME)
        for data_dict in data_list:
            if pixabaysearchutil.check_duplicate_document(data_dict) is False:
                connection_object.insert_document(data_dict)
        connection_object.close_connection()

    def data_ingestion_pipeline(self, search_query, product_name):
        data_list = self.get_data(search_query)
        self.logger.info("Fetched Data for %s " % product_name)
        parsed_data_list = self.parse_data(data_list,product_name=product_name)
        self.logger.info("Parsed Data for %s " % product_name)
        mongo_connector = dbutils.get_mongodb_connection()
        self.store_data(parsed_data_list, mongo_connector)
        self.logger.info("Stored data for product %s" % product_name)

PixabayDataIngestion().data_ingestion_pipeline("gold","TestGold")
