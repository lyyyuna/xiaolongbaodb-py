from collections import namedtuple

DEFAULT_LOGGER_NAME = 'Logger'

TreeConf = namedtuple('TreeConf', [
    'order',
    'page_size',
    'key_size',
    'value_size',
])

# network (big-edian)
ENDIAN = 'big'

PAGE_ADDRESS_LIMIT = 4

# bytes for storing length of each page
PAGE_LENGTH_LIMIT = 3

# bytes for storing per frame used in WAL module
FRAME_TYPE_LENGTH_LIMIT = 1