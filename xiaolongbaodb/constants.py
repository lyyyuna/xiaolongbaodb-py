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

# bytes for storing length of each page
PAGE_LENGTH_LIMIT = 3
