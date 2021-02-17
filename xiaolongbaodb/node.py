from abc import ABCMeta, abstractmethod
from xiaolongbaodb import constants
from xiaolongbaodb.constants import TreeConf
from xiaolongbaodb.btree import BTree

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

    def dump(self) -> bytes:
        pass


class OverflowNode(BaseBNode):
    def __init__(self, tree: BTree, tree_conf: TreeConf, page: int, data: bytes):
        pass