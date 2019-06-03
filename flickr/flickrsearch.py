from config import keys, constants
import flickrapi
from interface.interfacefactory import DataIngestionInterface
import json
from utils import logutils, dbutils
from flickr import flickrsearchutils


class FlickrDataIngestion(DataIngestionInterface):
    def __init__(self,api_key=keys.flickr['api_key'], api_secret=keys.flickr['api_secret']):
        self.flickr_client = flickrapi.FlickrAPI(api_key=api_key, secret=api_secret,format='json')
        self.logger = logutils.get_logger("Flickr Data Ingestion")

    def get_photo_sizes(self,photo_id):
        return self.flickr_client.photos.getSizes(photo_id=photo_id)

    def data_config(self, uri, **kwargs):
        pass

    def get_data(self, search_query, **kwargs):
        search_response = self.flickr_client.photos.search(text=search_query, per_page=20)
        search_result_response = json.loads(search_response)['photos']['photo']
        return search_result_response

    def parse_data(self, data_list, product_name='photoshop'):
        parsed_data_dict_list = list([])
        for photo_info in data_list:
            data_dict = dict({})
            data_dict['id'] = photo_info['id']
            data_dict['title'] = photo_info['title']
            data_dict['owner'] = photo_info['owner']
            data_dict['product'] = product_name
            size_url_list = json.loads(self.get_photo_sizes(data_dict['id']))['sizes']['size']
            for size_data in size_url_list:
                if size_data['label'] == 'Medium':
                    data_dict['source'] = size_data['source']
            parsed_data_dict_list.append(data_dict)
        return parsed_data_dict_list

    def store_data(self, data_list, connection_object):
        connection_object.set_collection(constants.FLICKR_COLLECTION_NAME)
        for data_dict in data_list:
            try:
                if flickrsearchutils.check_duplicate_document(data_dict) is False:
                    connection_object.insert_document(data_dict)
            except Exception:
                self.logger.error("Error while inserting document %s " % data_dict['title'])
        connection_object.close_connection()

    def data_ingestion_pipeline(self, search_query, product_name):
        data_list = self.get_data(search_query)
        self.logger.info("Data fetched for product %s " % product_name)
        parsed_data_list = self.parse_data(data_list, product_name=product_name)
        self.logger.info("Data parsed for product %s " % product_name)
        mongo_connector = dbutils.get_mongodb_connection()
        self.store_data(parsed_data_list, mongo_connector)
        self.logger.info("Data stored for product %s " % product_name)
