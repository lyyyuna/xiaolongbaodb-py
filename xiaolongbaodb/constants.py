from collections import namedtuple

DEFAULT_LOGGER_NAME = 'Logger'

TreeConf = namedtuple('TreeConf', [
    'order',
    'page_size',
    'key_size',
    'value_size',
])


