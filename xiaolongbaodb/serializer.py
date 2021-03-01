import json
import struct
from abc import ABCMeta, abstractstaticmethod
from typing import Union
from xiaolongbaodb.constants import INT_FORMAT


class NoSerializerError(Exception):
    pass


class Serializer(metaclass=ABCMeta):
    @abstractstaticmethod
    def serialize(obj: object) -> bytes:
        '''
        serialize a _key to bytes
        '''
        pass

    @abstractstaticmethod
    def deserialize(data: bytes) -> object:
        '''
        create a _key from rar bytes
        '''
        pass

    def __repr__(self) -> str:
        return '{}()'.format(self.__class__.__name__)


class IntSerializer(Serializer):
    @staticmethod
    def serialize(obj: int) -> bytes:
        return struct.pack(INT_FORMAT, obj)

    @staticmethod
    def deserialize(data: bytes) -> int:
        return struct.unpack(INT_FORMAT, data)[0]


class FloatSerializer(Serializer):
    pass


class StrSerializer(Serializer):
    pass


class DictSerializer(Serializer):
    pass


class ListSerializer(Serializer):
    pass


serializer_map = {
    int: IntSerializer(),
    float: FloatSerializer,
    str: StrSerializer(),
    dict: DictSerializer(),
    list: ListSerializer(),
}


def serializer_switcher(t: Union[int, float, str, dict, list]) -> Serializer:
    '''
    return corresponding serializer to arg type
    '''
    try:
        return serializer_map[t]
    except KeyError:
        raise NoSerializerError('no corresponding serializer')