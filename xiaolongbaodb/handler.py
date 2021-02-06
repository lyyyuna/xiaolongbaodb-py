import logging
from xiaolongbaodb.constants import *

logger = logging.getLogger(DEFAULT_LOGGER_NAME)


class FileHandler():
    def __init__(self, filename: str, tree_conf: TreeConf, cache_size: int):
        self._filename = filename
        self._tree_conf = tree_conf

        if cache_size <= 0:
            cache_size = 1024

        self._cache = LRUCache(capacity = cache_size)