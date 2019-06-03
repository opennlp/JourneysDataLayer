from config import constants
from utils import logutils
from dailymotionfetch.dailymotionsearch import DailyMotionDataIngestion

logger = logutils.get_logger("Dailymotion  fetch")


def dailymotion_fetch_and_store():
    daily_motion_search_agent = DailyMotionDataIngestion()
    for product, search_query_list in constants.PRODUCT_SEARCH_QUERY_DICT.items():
        for search_query in search_query_list:
            try:
                daily_motion_search_agent.data_ingestion_pipeline(search_query=search_query, product_name=product)
                logger.info("Successfully fetched data for product %s" % product)
            except Exception:
                logger.error("An error occurred during querying for %s" % search_query)
