import pyimgur
from config import keys, constants
from utils import dbutils, logutils
from interface.interfacefactory import DataIngestionInterface
from imgur import imgursearchutil


class ImgurDataIngestion(DataIngestionInterface):
    def __init__(self, client_id=keys.imgur['CLIENT_ID']):
        self.imgur_client = pyimgur.Imgur(client_id)
        self.logger = logutils.get_logger("Imgur Data Ingestion")

    def image_search_imgur(self,search_query):
        image_search_response = self.imgur_client.search_gallery(search_query)
        return image_search_response

    def data_config(self, uri, **kwargs):
        pass

    def get_data(self, search_query, **kwargs):
        image_search_response = self.imgur_client.search_gallery(search_query)
        return image_search_response

    def parse_data(self, data_list, product_name='photoshop'):
        parsed_data_dict_list = list([])
        for image_search in data_list:
            try:
                parsed_data_dict = dict({})
                if image_search.is_album:
                    image_search_link = image_search.images[0].link_medium_thumbnail
                else:
                    image_search_link = image_search.link_medium_thumbnail
                parsed_data_dict['source'] = image_search_link
                parsed_data_dict['author'] = image_search.author.name
                parsed_data_dict['comments'] = image_search.comment_count
                parsed_data_dict['downs'] = image_search.downs
                parsed_data_dict['ups'] = image_search.ups
                parsed_data_dict['views'] = image_search.views
                parsed_data_dict['points'] = image_search.points
                parsed_data_dict['title'] = image_search.title
                parsed_data_dict['description'] = image_search.description
                parsed_data_dict['topic'] = image_search.topic
                parsed_data_dict['product'] = product_name
                parsed_data_dict_list.append(parsed_data_dict)
            except Exception:
                self.logger.error("Error occured while inserting data")

        return parsed_data_dict_list

    def store_data(self, data_list, connection_object):
        connection_object.set_collection(constants.IMGUR_COLLECTION_NAME)
        for data_dict in data_list:
            try:
                if imgursearchutil.check_duplicate(data_dict) is False:
                    connection_object.insert_document(data_dict)
            except Exception:
                self.logger.error("Error occurred while inserting image %s" % data_dict['source'])
        connection_object.close_connection()

    def data_ingestion_pipeline(self, search_query, product_name):
        search_response = self.get_data(search_query)
        self.logger.info("Fetched Data for product %s " % product_name)
        parsed_data_list = self.parse_data(search_response, product_name=product_name)
        self.logger.info("Parsed Data for product %s " % product_name)
        mongo_connector = dbutils.get_mongodb_connection()
        self.store_data(parsed_data_list, mongo_connector)
        self.logger.info("Stored Data for product %s " % product_name)
