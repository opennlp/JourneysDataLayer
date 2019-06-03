from googleapiclient.discovery import build
from interface.interfacefactory import DataIngestionInterface
from utils import logutils, dbutils
from config import keys, constants
from youtube import youtubesearchutils


class YoutubeDataIngestion(DataIngestionInterface):
    def __init__(self,key=keys.youtube['DEVELOPER_KEY']):
        self.DEVELOPER_KEY = key
        self.YOUTUBE_API_SERVICE_NAME = "youtube"
        self.YOUTUBE_API_VERSION = "v3"
        self.logger = logutils.get_logger("Youtube Data Ingestion")

    def data_config(self, uri, **kwargs):
        youtube = build(self.YOUTUBE_API_SERVICE_NAME, self.YOUTUBE_API_VERSION,
                        developerKey=self.DEVELOPER_KEY)
        return youtube

    def get_data(self, youtube, **kwargs):
        search_query='adobe'
        max_results=25
        if 'search_query' in kwargs.keys():
            search_query = kwargs['search_query']
        if 'max_results' in kwargs.keys():
            max_results = kwargs['max_results']
        search_response = youtube.search().list(
            q=search_query,
            part="id,snippet",
            type='video',
            videoCaption='closedCaption',
            videoEmbeddable='true',
            maxResults=max_results
        ).execute()
        return search_response

    def parse_data(self, search_response,product_name='photoshop'):
        parsed_data_dict_list = []
        for search_result in search_response.get("items", []):
            if search_result["id"]["kind"] == "youtube#video":
                parsed_data_dict_list.append({'videoId': search_result["id"]["videoId"],
                                              'title': search_result["snippet"]["title"],
                                              'publishedAt': search_result["snippet"]["publishedAt"],
                                              'description': search_result["snippet"]["description"],
                                              'product': product_name})

        return parsed_data_dict_list

    def store_data(self, data_list, connection_object):
        connection_object.set_collection(constants.YOUTUBE_COLLECTION_NAME)
        for data_dict in data_list:
            try:
                if youtubesearchutils.check_duplicate_document(data_dict) is False:
                    connection_object.insert_document(data_dict)
            except Exception:
                self.logger.error("Error while inserting document %s " % data_dict['videoId'])
        connection_object.close_connection()

    def data_ingestion_pipeline(self,search_query, product_name):
        youtube_search_object = self.data_config(None)
        data_response = self.get_data(youtube_search_object, search_query=search_query)
        self.logger.info("Fetched data for product %s " % product_name)
        parsed_data_list = self.parse_data(data_response, product_name)
        self.logger.info("Parsed data for product %s " % product_name)
        mongo_connector = dbutils.get_mongodb_connection()
        self.store_data(parsed_data_list,mongo_connector)
        self.logger.info("Stored data for product %s " % product_name)
