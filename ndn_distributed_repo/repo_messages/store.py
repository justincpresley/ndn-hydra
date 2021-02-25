from ndn.encoding import *
from .message_base import MessageBodyBase

class StoreMessageBodyTypes:
    ID = 82
    NODE_ID = 83
    FAVOR = 84
    VALID_THRU = 85
    FILE = 90
    FILE_COPIES = 91
    FILE_SIZE = 92
    FILE_BLOCKS = 93

class FileTlv(TlvModel):
    name = NameField()
    copies = UintField(StoreMessageBodyTypes.FILE_COPIES, default=3)
    size = UintField(StoreMessageBodyTypes.FILE_SIZE)
    blocks = UintField(StoreMessageBodyTypes.FILE_BLOCKS)

class StoreMessageBodyTlv(TlvModel):
    insertion_id = UintField(StoreMessageBodyTypes.ID)
    node_id = BytesField(StoreMessageBodyTypes.NODE_ID)
    favor = UintField(StoreMessageBodyTypes.FAVOR)
    valid_thru = UintField(StoreMessageBodyTypes.VALID_THRU)
    file = ModelField(StoreMessageBodyTypes.FILE, FileTlv)

class StoreMessageBody(MessageBodyBase):
    def __init__(self, nid:str, seq:int, raw_bytes:bytes):
        super(StoreMessageBody, self).__init__(nid, seq)
        self.store_message_body = StoreMessageBodyTlv.parse(raw_bytes)

    async def apply(self, global_view):
        #TODO: apply this add msg to the global view
        return
