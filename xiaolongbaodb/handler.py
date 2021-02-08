import io
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
        self._wal = WAL(filename, tree_conf.page_size)

        # get the last available page


class WAL():
    '''
    Handler of write-ahead logging technique. WAL used to add a protective layer for data when some
    emergency happens during transaction. WAL provides an measurement to recover the lost data 
    next time user open the same database.
    '''
    def __init__(self, filename: str, page_size: int) -> None:
        self._filename = filename
        self._fd = util.open_database_file(filename=filename, suffix='.xdb.wal')
        self._page_size = page_size

        self._commited_pages = dict()
        self._uncommited_pages = dict()

        self._fd.seek(0, io.SEEK_END)
        if self._fd.tell() == 0:
            # if the wal log is empty, we need to create a new one
            self._create_header()
            self.needs_recovery = False
        else:
            logger.warning('found an existing WAL file, the database was not closed properly')
            self.needs_recovery = True
            self._load_wal()

    def _create_header(self):
        data = self._page_size.to_bytes(constants.PAGE_LENGTH_LIMIT, constants.ENDIAN)
        self._fd.seek(0)
        util.write_to_file(self._fd, data, True)

    def _load_wal(self):
        pass