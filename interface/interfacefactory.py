import abc


class DataIngestionInterface(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def get_data(self, uri, **kwargs):
        pass

    @abc.abstractmethod
    def data_config(self, uri, **kwargs):
        pass

    @abc.abstractmethod
    def parse_data(self, data_list):
        pass

    @abc.abstractmethod
    def store_data(self, data_list, connection_object):
        pass
