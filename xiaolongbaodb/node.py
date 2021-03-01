import functools
from abc import ABCMeta, abstractmethod
from xiaolongbaodb import constants
from xiaolongbaodb.constants import KEY_LENGTH_LIMIT, SERIALIZER_TYPE_LENGTH_LIMIT, TreeConf, VALUE_LENGTH_LIMIT
from xiaolongbaodb.btree import BTree

@functools.total_ordering
class KeyValPair(metaclass=ABCMeta):
    '''
    unit to store a paire of key-value, switch its serializer automatically by its type
    '''
    def __init__(self, tree_conf: TreeConf, key=None, value=None, data: bytes=None):
        self.tree_conf = tree_conf
        self._key = key
        self._val = value
        self.length = (KEY_LENGTH_LIMIT + self.tree_conf.key_size + VALUE_LENGTH_LIMIT + self.tree_conf.value_size + 2 * SERIALIZER_TYPE_LENGTH_LIMIT)

        if self._key is not None and self._val is not None:
            self.key_ser = 1


class BaseBNode(metaclass=ABCMeta):
    PAGE_TYPE = None

    @abstractmethod
    def load(self, data: bytes):
        pass

    @abstractmethod
    def dump(self) -> bytes:
        pass

    @classmethod
    def from_raw_data(cls, tree: BTree, tree_conf: TreeConf, page: int, data: bytes):
        '''
        construct node from the raw data, corresponding to the its node type
        '''
        node_type = int.from_bytes(data[0:constants.NODE_TYPE_LENGTH_LIMIT], constants.ENDIAN)
        if node_type == 0:
            return BNode(tree, tree_conf, page, data)
        elif node_type == 1:
            return OverflowNode(tree, tree_conf, page, data)
        elif node_type == 2:
            raise TypeError('deprecated data can only be used in page GC')
        else:
            raise TypeError('unknown node type: {type}'.format(type=node_type))


class BNode(BaseBNode):
    def __init__(self, tree: BTree, tree_conf: TreeConf, page: int, data: bytes):
        self.tree = tree
        self.tree_conf = tree_conf
        self.page = self.tree.next_available_page
        self.contents = []

    def dump(self) -> bytes:
        pass


class OverflowNode(BaseBNode):
    def __init__(self, tree: BTree, tree_conf: TreeConf, page: int, data: bytes):
        pass