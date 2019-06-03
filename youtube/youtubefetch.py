from config import constants
from utils import logutils
from youtube.youtubesearch import YoutubeDataIngestion

logger = logutils.get_logger("Youtube fetch")


def youtube_fetch_and_persist():
    youtube = YoutubeDataIngestion()
    for product_name, query_item_list in constants.PRODUCT_SEARCH_QUERY_DICT.items():
        for query_search in query_item_list:
            try:
                youtube.data_ingestion_pipeline(search_query=query_search, product_name=product_name)
                logger.info("Successfully executed youtube for all products")
            except Exception:
                logger.error("An error occurred for query term %s" % query_search)
