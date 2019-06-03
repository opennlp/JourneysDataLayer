from config import constants
from utils import logutils
from libraryio.libraryiosearch import LibraryIODataIngestion

logger = logutils.get_logger("LibraryIO Fetch")


def libraryio_fetch_and_persist():
    libraryio_search_agent = LibraryIODataIngestion()
    for product, search_query_list in constants.PRODUCT_SEARCH_QUERY_DICT.items():
        for search_query in search_query_list:
            try:
                libraryio_search_agent.data_ingestion_pipeline(search_term=search_query, product_name=product)
                logger.info("Successfully fetched data for product %s" % product)
            except Exception:
                logger.error("An error while fetching data for query %s" % search_query)
