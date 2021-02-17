import io
import logging
import enum
import os
import threading
from xiaolongbaodb.node import BNode, BaseBNode
from xiaolongbaodb import constants, util, btree

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
        self._fd.seek(0, io.SEEK_END)
        last_byte = self._fd.tell()
        self.last_page = int(last_byte / self._tree_conf.page_size)
        self._auto_commit = True
        self._page_GC = list(self._load_page_gc())

    def _load_page_gc(self):
        '''
        load all deprecated page used before into the memory
        '''
        for offset in range(1, self.last_page):
            page_start = offset * self._tree_conf.page_size
            page_type = util.read_from_file(self._fd, page_start, constants.NODE_TYPE_LENGTH_LIMIT)
            if page_type == 2: # _PageType.DEPRECATED_PAGE._value==2
                yield offset

    @property
    def write_transaction(self):
        class WriteTransaction:
            def __enter__(_self):
                self._lock.acquire()

            def __exit__(_self, exc_type, exc_val, exc_tb):
                if exc_type:
                    self._wal.rollback()
                    self._cache.clear()
                else:
                    if self._auto_commit:
                        self._wal.commit()
                self._lock.release()

        return WriteTransaction()

    @property
    def read_transaction(self):
        class ReadTransaction:
            def __enter__(_self):
                self._lock.acquire()

            def __exit__(_self):
                self._lock.release()

        return ReadTransaction()

    def _fd_seek_end(self):
        self._fd.seek()

    def _read_page_data(self, page: int) -> bytes:
        '''
        read no.x page raw binary data from the db file 
        '''
        page_start = page * self._tree_conf.page_size
        data = util.read_from_file(self._fd, page_start, page_start + self._tree_conf.page_size)
        return data

    def _write_page_data(self, page: int, page_data: bytes, f_sync: bool = False):
        '''
        write no.x page raw binary data into the db file
        '''
        assert len(page_data) == self._tree_conf.page_size, 'length of the page size does not match the page_data'

        page_start = page * self._tree_conf.page_size
        self._fd.seek(page_start)
        util.write_to_file(self._fd, page_data, f_sync=f_sync)

    def get_meta_tree_conf(self) -> tuple:
        '''
        read former recorded tree conf from the db file, as first page data
        '''
        try:
            data = self._read_page_data(0)
        except util.EndOfFileError:
            raise ValueError('meta tree data not complete')
        root_page = int.from_bytes(data[0:constants.PAGE_ADDRESS_LIMIT], constants.ENDIAN)
        order_end = constants.PAGE_ADDRESS_LIMIT + 1
        order = int.from_bytes(data[constants.PAGE_ADDRESS_LIMIT:order_end], constants.ENDIAN)
        page_size_end = order_end + constants.PAGE_LENGTH_LIMIT
        page_size = int.from_bytes(data[order_end:page_size_end], constants.ENDIAN)
        key_size_end = page_size_end + constants.KEY_LENGTH_LIMIT
        key_size = int.from_bytes(data[page_size_end:key_size_end], constants.ENDIAN)
        value_size_end = key_size_end + constants.VALUE_LENGTH_LIMIT
        value_size = int.from_bytes(data[key_size_end:value_size_end], constants.ENDIAN)

        if order != self._tree_conf.order:
            order = self._tree_conf.order
        self._tree_conf = constants.TreeConf(order, page_size, key_size, value_size)

        return root_page, self._tree_conf

    def get_node(self, page: int, tree: btree.BTree):
        '''
        try to get node from the cache to avoid extra i/o,
        if not exist, get and load from the db file
        '''
        node = self._cache.get(page)
        if node:
            return node

        data = self._wal.get_page(page)
        if not data:
            data = self._read_page_data(page)
        
        node = BaseBNode.from_raw_data(tree, self._tree_conf, page, data)
        self._cache[page] = node
        return node

    def set_node(self, node: BNode):
        '''
        add & update node dumped data into the db file and also update the cache 
        '''
        self._wal.set_page(node.page, node.dump())
        self._cache[node.page] = node

    def ensure_root_block(self, root: BNode):
        '''
        sync current root node info with both memory and disk
        '''
        self.set_node(root)
        self.set_meta_tree_conf(root.page, root.tree_conf)
        self.commit()

    def set_meta_tree_conf(self, page: int, tree_conf: constants.TreeConf):
        '''
        set current tree conf into the db file, record in the first page,
        file-sync is necessary.
        '''
        self._tree_conf = tree_conf
        length = constants.PAGE_ADDRESS_LIMIT + 1 + constants.PAGE_LENGTH_LIMIT + constants.KEY_LENGTH_LIMIT + constants.VALUE_LENGTH_LIMIT
        data = (
            page.to_bytes(constants.PAGE_ADDRESS_LIMIT, constants.ENDIAN) +
            self._tree_conf.order.to_bytes(1, constants.ENDIAN) + 
            self._tree_conf.page_size.to_bytes(constants.PAGE_LENGTH_LIMIT, constants.ENDIAN) + 
            self._tree_conf.key_size.to_bytes(constants.KEY_LENGTH_LIMIT, constants.ENDIAN) +
            self._tree_conf.value_size.to_bytes(constants.VALUE_LENGTH_LIMIT, constants.ENDIAN) +
            bytes(self._tree_conf.page_size - length) # padding
        )
        self._write_page_data(0, data, f_sync=True)

    def _takeout_deprecated_page(self) -> int:
        '''
        if GC still has pages, take the smallest one
        '''
        if self._page_GC:
            return self._page_GC.pop(0)
        return None

    @property
    def next_available_page(self) -> int:
        # try to get one page from the GC first, else get one by increase last_page
        dep_page = self._takeout_deprecated_page()
        if dep_page:
            return dep_page
        else:
            self.last_page += 1
            return self.last_page


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

    def _add_frame(self, frame_type: FrameType, page: int = None, page_data: bytes = None):
        if frame_type is FrameType.PAGE and (not page or not page_data):
            raise ValueError('page frame without page or page data')
        if page_data and len(page_data) != self._page_size:
            raise ValueError('page data is different from the page size')
        if not page:
            page = 0
        if frame_type is not FrameType.PAGE:
            page_data = b''
        data = (
            frame_type.value.to_bytes(constants.FRAME_TYPE_LENGTH_LIMIT, constants.ENDIAN) + 
            page.to_bytes(constants.PAGE_ADDRESS_LIMIT, constants.ENDIAN) +
            page_data
        )

        if page in self._commited_pages.keys() and frame_type == FrameType.PAGE:
            # if this page has wrote into WAL before, overwrite it, or the size of .wal file will boom.
            page_start = self._commited_pages[page]
            seek_start = page_start - constants.FRAME_TYPE_LENGTH_LIMIT - constants.PAGE_ADDRESS_LIMIT
            self._fd.seek(seek_start)
        else:
            self._fd.seek(0, io.SEEK_END)
        util.write_to_file(self._fd, data, f_sync=frame_type != FrameType.PAGE)
        self._index_frame(frame_type, page, self._fd.tell() - self._page_size)

    def set_page(self, page: int, page_data: bytes):
        self._add_frame(FrameType.PAGE, page, page_data)

    def commit(self):
        '''
        commit is no-op when there is no uncommitted pages.
        '''
        if self._uncommited_pages:
            self._add_frame(FrameType.COMMIT)

    def rollback(self):
        if self._uncommited_pages:
            self._add_frame(FrameType.ROLLBACK)

    def get_page(self, page: int) -> bytes:
        page_start = None
        for store in (self._uncommited_pages, self._commited_pages):
            page_start = store.get(page)
            if page_start:
                break
        
        if not page_start:
            return b''


        return util.read_from_file(self._fd, page_start, page_start + self._page_size)

    def checkpoint(self):
        '''
        transfer the modified data back to the test_tree and close the WAL
        '''
        if self._uncommited_pages:
            logger.warning('close WAL with uncommited data, discarding it')

        util.file_flush_and_sync(self._fd)

        for page, page_start in self._commited_pages.items():
            page_data = util.read_from_file(self._fd, page_start, page_start + self._page_size)
            yield page, page_data

        self._fd.close()
        os.unlink(self._filename + '.xdb.wal')