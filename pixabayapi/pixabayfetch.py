from config import constants
from utils import logutils
from pixabayapi.pixabaysearch import PixabayDataIngestion

logger = logutils.get_logger("Pixabay Fetch")


def pixabay_fetch_and_persist():
    pixabay_search_agent = PixabayDataIngestion()
    for product, search_query_list in constants.PRODUCT_SEARCH_QUERY_DICT.items():
        for search_query in search_query_list:
            try:
                pixabay_search_agent.data_ingestion_pipeline(search_query=search_query, product_name=product)
                logger.info("Successfully fetched data for %s" % product)
            except Exception:
                logger.error("Error occurred while fetching data for search query %s" % search_query)
