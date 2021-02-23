import logging
from xiaolongbaodb.constants import *
from xiaolongbaodb.handler import FileHandler
from xiaolongbaodb.node import BNode

logger = logging.getLogger(DEFAULT_LOGGER_NAME)


class BTree():
    LEAF = BNode
    __slots__ = ('_file_name', '_order', '_root')
    def __init__(self, file_name: str = 'xiaolongbao.db', order: int = 100, page_size: int = 8192, key_size: int = 16, value_size: int = 64, cache_szie=1024):
        self._file_name = file_name
        self._tree_conf = TreeConf(order=order, page_size=page_size, key_size=page_size, value_size=value_size)
        self.handler = FileHandler()
        self._order = order
        try:
            with self.handler.read_transaction:
                meta_root_page, meta_tree_conf = self.handler.get_meta_tree_conf()
        except ValueError:
            # init a empty tree
            with self.handler.write_transaction:
                self._root = self._bottom = self.LEAF(self, self._tree_conf)
        else:
            with self.handler.read_transaction:
                self._root, self._tree_conf = self.handler.get_node(meta_root_page, tree=self), meta_tree_conf

        self._closed = False

    @property
    def next_available_page(self) -> int:
        '''
        used for upper layer
        '''
        return self.handler.next_available_page

    def _path_to(self, key) -> list:
        '''
        get the path from the root to the target node
        :return: list of node-path from root to key node
        '''
        with self.handler.read_transaction:
            current_node = self._root
            ancestry = []

            while getattr(current_node, 'children', None):
                pass