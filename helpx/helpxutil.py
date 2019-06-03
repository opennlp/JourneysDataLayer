from config import constants
import requests
from bs4 import BeautifulSoup
from utils import dbutils, logutils


def get_url_mapping_list(url=constants.HELPX_SITEMAP_URL):
    result = requests.get(url)
    content=result.content
    soup = BeautifulSoup(content, "html.parser")
    anchor_list = soup.find_all("a")
    link_timestamp_dict = dict({})
    for anchor_tag in anchor_list:
        link_timestamp_dict[anchor_tag.attrs['href']] = sanitize_date_string(anchor_tag.next_sibling.string)
    return link_timestamp_dict


def sanitize_date_string(date_string):
    date_string = date_string.strip()
    separator = 'T'
    index_of_sep = date_string.find(separator)
    if index_of_sep != -1:
        date_string = date_string[:index_of_sep]
    return date_string


def get_product_name_url(url):
    return url.strip().split('/')[3].replace('-', '_').lower()


def one_time_populate_links(url=constants.HELPX_SITEMAP_URL):
    logger = logutils.get_logger('One Time Populate')
    url_timestamp_dict = get_url_mapping_list(url)
    mongo_connector = dbutils.get_mongodb_connection()
    mongo_connector.set_collection(constants.HELPX_URL_TIMESTAMP_COLLECTION_NAME)
    for key, value in url_timestamp_dict.items():
        try:
            mongo_connector.insert_document({'url': key, 'timestamp': value})
        except:
            logger.error("Error while Inserting document")
        finally:
            mongo_connector.close_connection()
