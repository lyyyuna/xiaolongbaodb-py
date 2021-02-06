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

