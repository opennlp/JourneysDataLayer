from helpx.helpxutil import get_url_mapping_list
from helpx.helpxdata import HelpXDataIngestion
from utils import logutils

logger = logutils.get_logger("HelpX Fetch Data")


def helpx_fetch_schedule():
    url_timestamp_dict = get_url_mapping_list()
    helpx_pipeline = HelpXDataIngestion()
    for url, timestamp in url_timestamp_dict.items():
        try:
            helpx_pipeline.data_ingestion_pipeline(url, timestamp=timestamp)
            logger.info("Pipeline Inserted for uri %s " % url)
        except:
            logger.error("Error occurred for url %s" % url)
