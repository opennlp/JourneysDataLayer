from config import constants
from utils import logutils
from twitter import twittersearch

logger = logutils.get_logger("Twitter fetch")


def twitter_search_and_persist():
    twitter_search_agent = twittersearch.TwitterDataIngestion()
    for product, search_query_list in constants.PRODUCT_SEARCH_QUERY_DICT.items():
        for search_query in search_query_list:
            try:
                twitter_search_agent.data_ingestion_pipeline(search_query,product=product)
                logger.info("Twitter search successfully executed for all channels")
            except Exception:
                logger.error("An exception occurred while querying %s " % search_query)
