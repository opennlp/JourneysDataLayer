from config import constants
from utils import logutils
from news.searchnews import NewsDataIngestion

logger = logutils.get_logger("News Fetch")


def news_fetch_and_persist():
    news_search_agent = NewsDataIngestion()
    for product, search_query_list in constants.PRODUCT_SEARCH_QUERY_DICT.items():
        for search_query in search_query_list:
            try:
                news_search_agent.data_ingestion_pipeline(search_query, product=product)
                logger.info("Successfully fetched data for product %s" % product)
            except Exception:
                logger.error("An error occurred for the search term %s" % search_query)
