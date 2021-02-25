from ndn.encoding import *
from .message_base import MessageBodyBase

class ClaimMessageBodyTypes:
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
    copies = UintField(ClaimMessageBodyTypes.FILE_COPIES, default=3)
    size = UintField(ClaimMessageBodyTypes.FILE_SIZE)
    blocks = UintField(ClaimMessageBodyTypes.FILE_BLOCKS)

class ClaimMessageBodyTlv(TlvModel):
    insertion_id = UintField(ClaimMessageBodyTypes.ID)
    node_id = BytesField(ClaimMessageBodyTypes.NODE_ID)
    favor = UintField(ClaimMessageBodyTypes.FAVOR)
    valid_thru = UintField(ClaimMessageBodyTypes.VALID_THRU)
    file = ModelField(ClaimMessageBodyTypes.FILE, FileTlv)

class ClaimMessageBody(MessageBodyBase):
    def __init__(self, nid:str, seq:int, raw_bytes:bytes):
        super(ClaimMessageBody, self).__init__(nid, seq)
        self.claim_message_body = ClaimMessageBodyTlv.parse(raw_bytes)

    async def apply(self, global_view):
        #TODO: apply this add msg to the global view
        return
