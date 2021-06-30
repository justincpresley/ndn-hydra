import logging


class MessageBodyBase:
    def __init__(self, nid:str, seq:int):
        self.nid = nid
        self.seq = seq
        self.logger = logging.getLogger()

    async def apply(self, global_view):
        raise NotImplementedError