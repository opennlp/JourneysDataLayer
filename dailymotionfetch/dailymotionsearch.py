import dailymotion
from dailymotionfetch import dailymotionsearchutils
from interface.interfacefactory import DataIngestionInterface
from utils import dbutils, logutils
from config import constants


class DailyMotionDataIngestion(DataIngestionInterface):
    def __init__(self):
        self.daily_client = dailymotion.Dailymotion()
        self.logger = logutils.get_logger("Daily Motion Ingestion")

    def data_config(self, search_query, **kwargs):
        base_url = '/videos?fields=allow_embed,channel,created_time,description,embed_url,id,likes_total,owner,title,&limit=100'
        final_url = base_url + '&search=' + search_query.strip()
        return final_url

    def get_data(self, uri, **kwargs):
        return self.daily_client.get(uri)

    def parse_data(self, response_json, product_name='photoshop'):
        response_data_list = response_json['list']
        parsed_data_list = list([])
        for response_data in response_data_list:
            try:
                parsed_data_dict = dict({})
                parsed_data_dict['product'] = product_name
                parsed_data_dict['channel'] = response_data['channel']
                parsed_data_dict['likes'] = response_data['likes_total']
                parsed_data_dict['title'] = response_data['title']
                parsed_data_dict['description'] = response_data['description']
                parsed_data_dict['created_time'] = dailymotionsearchutils.get_python_date_from_unix_timestamp(response_data['created_time'])
                parsed_data_dict['embed_url'] = response_data['embed_url']
                parsed_data_dict['owner'] = response_data['owner']
                parsed_data_dict['id'] = response_data['id']
                parsed_data_list.append(parsed_data_dict)
            except Exception:
                self.logger.error("Error occurred while inserting data for product %s " % product_name)
        return parsed_data_list

    def store_data(self, data_list, connection_object):
        connection_object.set_collection(constants.DAILYMOTION_COLLECTION_NAME)
        for data_dict in data_list:
            try:
                if dailymotionsearchutils.check_duplicate_document(data_dict) is False:
                    connection_object.insert_document(data_dict)
            except Exception:
                self.logger.error("Exception occurred while inserting data %s " % data_dict['title'])
        connection_object.close_connection()

    def data_ingestion_pipeline(self, search_query, product_name):
        uri = self.data_config(search_query)
        self.logger.info("Data Source Configured for product %s " % product_name)
        response_data = self.get_data(uri)
        self.logger.info("Data fetched for product %s " % product_name)
        parsed_data_list = self.parse_data(response_data, product_name=product_name)
        self.logger.info("Data parsed for product %s " % product_name)
        mongo_connector = dbutils.get_mongodb_connection()
        self.store_data(parsed_data_list, mongo_connector)
        self.logger.info("Data stored for product %s " % product_name)
