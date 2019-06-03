from config import constants
from utils import logutils
from imgur.imgursearch import ImgurDataIngestion

logger = logutils.get_logger("Imgur Data Fetch")


def imgur_fetch_and_persist():
    imgur_search_agent = ImgurDataIngestion()
    for product_name, search_query_list in constants.PRODUCT_SEARCH_QUERY_DICT.items():
        for search_query in search_query_list:
            try:
                imgur_search_agent.data_ingestion_pipeline(search_query=search_query, product_name=product_name)
                logger.info("Successfully fetched data for product %s" % product_name)
            except Exception:
                logger.error("An exception occured while querying for %s" % search_query)
