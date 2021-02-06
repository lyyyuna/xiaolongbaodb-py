import logging
from xiaolongbaodb.constants import *

logger = logging.getLogger(DEFAULT_LOGGER_NAME)


class BTree():
    __slots__ = ('_file_name', '_order', '_root')
    def __init__(self, file_name: str = 'xiaolongbao.db', order: int = 100, page_size: int = 8192, key_size: int = 16, value_size: int = 64, cache_szie=1024):
        self._file_name = file_name
        self._tree_conf = TreeConf(order=order, page_size=page_size, key_size=page_size, value_size=value_size)
        self.handler = FileHandler()
        self._order = order
        try:
            pass
        except ValueError:
            pass
        else:
            pass

