import logging
import threading
from xiaolongbaodb import constants, util

logger = logging.getLogger(constants.DEFAULT_LOGGER_NAME)


class FileHandler():
    __slots__ = ('_filename', '_tree_conf', '_fd', '_lock')
    def __init__(self, filename: str, tree_conf: constants.TreeConf, cache_size: int):
        self._filename = filename
        self._tree_conf = tree_conf

        if cache_size <= 0:
            cache_size = 1024

        self._cache = util.LRUCache(capacity = cache_size)
        self._fd = util.open_database_file(self._filename)
        self._lock = threading.Lock()

        # get the last available page


class WAL():
    def __init__(self, filename: str, page_size: int) -> None:
        self._filename = filename
        self._fd = util.open_database_file(filename=filename, suffix='.xdb.wal')
        self._page_size = page_size