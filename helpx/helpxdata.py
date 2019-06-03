from interface.interfacefactory import DataIngestionInterface
import requests
from bs4 import BeautifulSoup, NavigableString
import re
from config import constants
from utils import commonutils, dbutils, logutils, dateutils
from helpx import helpxutil
from exception.customexception import DataException


class HelpXDataIngestion(DataIngestionInterface):

    def __init__(self):
        self.logger = logutils.get_logger("HelpXDataIngestion")

    def get_data(self, uri, **kwargs):
        result = requests.get(uri)
        timestamp = dateutils.get_current_date_in_string()
        if "timestamp" in kwargs.keys():
            timestamp = kwargs['timestamp']
        if self.check_for_latest_update(uri, timestamp):
            content = result.content
            return content
        return False

    @staticmethod
    def traverse_between_tags(cur, end):
        text_between_tags = ''
        while cur and cur != end:
            if isinstance(cur, NavigableString):
                text = cur.strip()
                if len(text):
                    text_between_tags += text
            cur = cur.next_element
        return text_between_tags

    def data_config(self, uri, **kwargs):
        return uri

    def parse_data(self, data):
        soup = BeautifulSoup(data, "html.parser")
        parsed_data_dict = dict({})
        heading_dict = dict({})
        page_title = str(soup.title.string).lower()
        parsed_data_dict['title'] = page_title
        heading_text_list = soup.find_all('article')[0].find_all(re.compile('h[1-6]'))
        for index in range(len(heading_text_list)-1):
            text = self.traverse_between_tags(heading_text_list[index], heading_text_list[index+1]).lower()
            associated_heading = heading_text_list[index].string.strip().lower()
            cleaned_text = commonutils.remove_stopwords(text, constants.CUSTOM_STOPWORD_LIST)
            cleaned_text = cleaned_text.replace(associated_heading,' ')
            associated_heading = commonutils.remove_stopwords(associated_heading, constants.CUSTOM_STOPWORD_LIST)
            if len(cleaned_text) > 25:
                heading_dict[associated_heading] = cleaned_text
        parsed_data_dict['heading_dict'] = heading_dict
        return parsed_data_dict

    def store_data(self, data, connection_object):
        try:
            connection_object.set_collection(constants.HELPX_COLLECTION_NAME)
            connection_object.insert_document(data)
        except:
            self.logger.error("Error while inserting data %s" % str(data))
            raise DataException('Error While Inserting in database')
        finally:
            connection_object.close_connection()
            self.logger.info("Successfully inserted into database")

    def data_ingestion_pipeline(self, uri, **kwargs):
        if 'timestamp' in kwargs:
            timestamp = kwargs['timestamp']
        else:
            timestamp = dateutils.get_current_date_in_string()
        config_uri = self.data_config(uri)
        self.logger.debug('Configured for url %s ' % uri)
        raw_data = self.get_data(config_uri, timestamp=timestamp)
        if raw_data is not False:
            self.logger.debug('Fetched Data for url %s ' % uri)
            parsed_data = self.parse_data(raw_data)
            self.logger.debug('Parsed data for url %s ' % uri)
            parsed_data['product'] = helpxutil.get_product_name_url(uri)
            parsed_data['uri'] = uri
            sha_digest_fingerprint_text = str(parsed_data['heading_dict'])
            sha_digest_fingerprint_text = sha_digest_fingerprint_text.encode('ascii',errors='ignore')
            if self.check_for_sha_digest(sha_digest_fingerprint_text,parsed_data['product']) is False:
                parsed_data['sha1'] = commonutils.get_sha_hash(sha_digest_fingerprint_text)
                mongo_connector = dbutils.get_mongodb_connection()
                self.store_data(parsed_data, mongo_connector)
                self.logger.debug('Inserted Parsed Data for url %s ' % uri)
        return True

    @staticmethod
    def check_for_latest_update(url,timestamp_string):
        mongo_connector = dbutils.get_mongodb_connection()
        mongo_connector.set_collection(constants.HELPX_URL_TIMESTAMP_COLLECTION_NAME)
        query = dict({'url': url})
        cursor = mongo_connector.find_document(query)
        if cursor.count() > 0:
            for document in cursor:
                stored_timestamp = document['timestamp']
                if dateutils.compare_dates(timestamp_string, stored_timestamp):
                    mongo_connector.update_document(dict({"$set": {"timestamp": timestamp_string}}), query)
                    mongo_connector.close_connection()
                    return True
                else:
                    return False
        else:
            mongo_connector.insert_document({'url': url, 'timestamp': timestamp_string})
            mongo_connector.close_connection()
            return True

    @staticmethod
    def check_for_sha_digest(text, product_name):
        query = dict({'product': product_name,'sha1': commonutils.get_sha_hash(text)})
        mongo_connector = dbutils.get_mongodb_connection()
        mongo_connector.set_collection(constants.HELPX_COLLECTION_NAME)
        if mongo_connector.check_document(query):
            mongo_connector.close_connection()
            return True
        return False
