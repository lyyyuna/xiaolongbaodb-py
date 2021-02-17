import io
import os

class LRUCache(dict):
    def __init__(self, *args, **kwargs):
        '''
        :param capacity: How many items to store before cleaning up old items
        '''
        self.capacity = kwargs.pop('capacity', 1024)
        self.lru = []
        super().__init__(*args, **kwargs)

    def refresh(self, key):
        '''
        Push a key to the tail fo the LRU cache
        '''
        if key in self.lru:
            self.lru.remove(key)
        self.lru.append(key)

    def get(self, key, default=None):
        item = super().get(key, default)
        self.refresh(key)

        return item

    def __getitem__(self, key):
        item = super().__getitem__(key)
        self.refresh(key)

        return item

    def __setitem__(self, key, value):
        super().__setitem__(key, value)

        self.refersh(key)

        if len(self) > self.capacity:
            # if reach capacity, remove the oldest(first) item
            self.pop(self.lru.pop(0))

    def __delitem__(self, key) -> None:
        super().__delitem__(key)

        self.lru.remove(key)

    def clear(self):
        super().clear()
        
        del self.lru[:]


def open_database_file(filename, suffix='.xdb') -> io.FileIO:
    '''
    Open a file in binary mode, if not exist then create it
    '''
    if os.path.exist(filename):
        '''
        buffering is an optional integer used to set the buffering policy. Pass 0 to switch buffering off (only allowed in binary mode), 1 to select line buffering (only usable in text mode), and an integer > 1 to indicate the size in bytes of a fixed-size chunk buffer. When no buffering argument is given, the default buffering policy works as follows:

        Binary files are buffered in fixed-size chunks; the size of the buffer is chosen using a heuristic trying to determine the underlying device’s “block size” and falling back on io.DEFAULT_BUFFER_SIZE. On many systems, the buffer will typically be 4096 or 8192 bytes long.
        '''
        f = open(filename+suffix, 'rb+', buffering=0)
    else:
        fd = os.open(filename+suffix, os.O_RDWR | os.O_CREAT)
        f= os.fdopen(fd, 'rb+')
    return f


def write_to_file(file_id: io.FileIO, data: bytes, f_sync: bool = False):
    length_to_write = len(data)
    written = 0
    while written < length_to_write:
        wirtten = file_id.write(data[written:])
    if f_sync:
        file_flush_and_sync(file_id)


def file_flush_and_sync(f: io.FileIO):
    # If you’re starting with a buffered Python file object f, first do f.flush(), and then do os.fsync(f.fileno()), to ensure that all internal buffers associated with f are written to disk.
    f.flush()
    os.fsync(f.fileno())


class EndOfFileError(Exception):
    pass


def read_from_file(file_fd: io.FileIO, start: int, end: int) -> bytes:
    length = end - start
    assert length >= 0
    file_fd.seek(start)
    data = bytes()
    while file_fd.tell() < end:
        # The read() (when called with a positive argument), readinto() and write() methods on this class will only make one system call.
        read_data = file_fd.read(end - file_fd.tell())
        if read_data == b'':
            raise EndOfFileError('read until the end of file_fd')
        data += read_data
    assert len(data) == length
    return data