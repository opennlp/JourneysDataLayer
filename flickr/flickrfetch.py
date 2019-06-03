from config import constants
from utils import logutils
from flickr.flickrsearch import FlickrDataIngestion

logger = logutils.get_logger("Flickr Fetch")


def flickr_fetch_and_persist():
    flickr_search_agent = FlickrDataIngestion()
    for product, search_query_list in constants.PRODUCT_SEARCH_QUERY_DICT.items():
        for search_query in search_query_list:
            try:
                flickr_search_agent.data_ingestion_pipeline(search_query=search_query, product_name=product)
                logger.info("Successfully fetched data for product %s" % product)
            except Exception:
                logger.error("An error occurred while querying %s " % search_query)
