from config import constants
from utils import logutils
from reddit import redditsearch

logger = logutils.get_logger("Reddit fetch")


def reddit_fetch_and_persist():
    reddit_search_agent = redditsearch.RedditDataIngestion()
    for product, search_query_list in constants.PRODUCT_SEARCH_QUERY_DICT.items():
        for search_query in search_query_list:
            reddit_search_agent.data_ingestion_pipeline(search_query, product=product)
