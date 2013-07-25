class simple:
    ''' simple is a very basic in memory data store

    TODO: values expire unless they are refreshed.
    '''
    def __init__(self):
        self.hash_value = {}
        self.hash_key   = {}


    def store(self, key, key_hash, value):
        self.hash_value[key_hash] = value
        self.hash_key[key_hash]   = key

    def retrieve(self, key_hash):
        if key_hash in self.hash_value:
            return self.hash_value[key_hash]
        else:
            return None

    def remove(self, key_hash):
        if key_hash in self.hash_value:
            del self.hash_value[key_hash]

        if key_hash in self.hash_key:
            del self.hash_key[key_hash]
