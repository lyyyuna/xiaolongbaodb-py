import io
import logging
import enum
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


class FrameType(enum.Enum):
    PAGE = 1
    COMMIT = 2
    ROLLBACK = 3


class WAL():
    '''
    Handler of write-ahead logging technique. WAL used to add a protective layer for data when some
    emergency happens during transaction. WAL provides an measurement to recover the lost data 
    next time user open the same database.
    '''

    FRAME_HEADER_LENGTH = constants.FRAME_TYPE_LENGTH_LIMIT + constants.PAGE_ADDRESS_LIMIT

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
        '''
        load previous WAL generated when B Tree closed accidentally.
        '''
        self._fd.seek(0)
        header_data = util.read_from_file(self, 0, constants.PAGE_LENGTH_LIMIT)
        assert int.from_bytes(header_data, constants.ENDIAN) == self._page_size

        while True:
            try:
                self._load_next_frame()
            except util.EndOfFileError:
                break
        if self._uncommited_pages:
            logger.warning('WAL has uncommited data, discarding it')
            self._uncommited_pages = dict()

    def _load_next_frame(self):
        start = self._fd.tell()
        end = start + self.FRAME_HEADER_LENGTH
        data = util.read_from_file(self._fd, start, end)

        frame_type = int.from_bytes(data[0:constants.FRAME_TYPE_LENGTH_LIMIT], constants.ENDIAN)
        frame_type = FrameType(frame_type)
        if frame_type is FrameType.PAGE:
            self._fd.seek(end + self._page_size)

        page = int.from_bytes(data[constants.FRAME_TYPE_LENGTH_LIMIT:constants.FRAME_TYPE_LENGTH_LIMIT+constants.PAGE_ADDRESS_LIMIT])
        self._index_frame(frame_type, page, end)

    def _index_frame(self, frame_type: FrameType, page: int, page_start: int):
        if frame_type is FrameType.PAGE:
            self._uncommited_pages[page] = page_start
        elif frame_type is FrameType.COMMIT:
            self._commited_pages.update(self._uncommited_pages)
            self._uncommited_pages = dict()
        elif frame_type is FrameType.ROLLBACK:
            self._uncommited_pages = dict()
        else:
            assert False
