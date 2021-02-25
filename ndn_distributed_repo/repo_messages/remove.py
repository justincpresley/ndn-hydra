from ndn.encoding import *
from .message_base import MessageBodyBase

class RemoveMessageBodyTypes:
    ID = 82
    NODE_ID = 83
    FAVOR = 84
    VALID_THRU = 85
    FILE = 90
    FILE_COPIES = 91
    FILE_SIZE = 92
    FILE_BLOCKS = 93
    FILE_SEQ = 95

class FileTlv(TlvModel):
    name = NameField()
    copies = UintField(RemoveMessageBodyTypes.FILE_COPIES, default=3)
    size = UintField(RemoveMessageBodyTypes.FILE_SIZE)
    blocks = UintField(RemoveMessageBodyTypes.FILE_BLOCKS)

class RemoveMessageBodyTlv(TlvModel):
    deletion_id = UintField(RemoveMessageBodyTypes.ID)
    node_id = BytesField(RemoveMessageBodyTypes.NODE_ID)
    favor = UintField(RemoveMessageBodyTypes.FAVOR)
    valid_thru = UintField(RemoveMessageBodyTypes.VALID_THRU)
    file = ModelField(RemoveMessageBodyTypes.FILE, FileTlv)
    file_seq = UintField(RemoveMessageBodyTypes.FILE_SEQ)

class RemoveMessageBody(MessageBodyBase):
    def __init__(self, nid:str, seq:int, raw_bytes:bytes):
        super(RemoveMessageBody, self).__init__(nid, seq)
        self.remove_message_body = RemoveMessageBodyTlv.parse(raw_bytes)

    async def apply(self, global_view):
        #TODO: apply this add msg to the global view
        return
