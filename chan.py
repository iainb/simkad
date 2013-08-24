from gevent.queue import Queue

class SelectQueue:
    def __init__(self, channel, queue):
        self.channel = channel
        self.queue   = queue

    def put(self, message, **kwargs):
        ''' send a message on the given channel name '''
        m = { 'channel' : self.channel, 'message' : message }
        self.queue.put(m, **kwargs)

    def get(self):
        ''' You can only write to this queue '''
        raise NotImplementedError

class SelectChan:
    ''' SelectChan is a basic multiplexing channel based on the eventlet Queue
    class. All messages sent (put)on the queue are required to have a 'channel'
    which they are sent on. When we recieve (get) a message the channel which
    it was sent on is returned.
    '''
    def __init__(self):
        self.queue = Queue()

    def get(self, *args, **kwargs):
        m = self.queue.get(*args, **kwargs)
        return m['channel'], m['message']

    def put(self, channel, message):
        m = { 'channel' : channel, 'message' : message }
        self.queue.put(m)

    def fetch_chan(self, channel):
        q = SelectQueue(channel, self.queue)
        return q
