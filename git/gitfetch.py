from config import constants
from utils import logutils
from git.gitsearch import GitDataIngestion

logger = logutils.get_logger("Git Data Fetch")


def git_fetch_and_persist():
    git_search_agent = GitDataIngestion()
    for product, search_query_list in constants.PRODUCT_SEARCH_QUERY_DICT.items():
        for search_query in search_query_list:
            try:
                git_search_agent.data_ingestion_pipeline(search_query=search_query, product=product)
                logger.info("Successfully inserted data for product %s" % product)
            except Exception:
                logger.error("An error occurred while querying for %s " % search_query)
