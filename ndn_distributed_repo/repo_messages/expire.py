from ndn.encoding import *
from .message_base import MessageBodyBase

class ExpireMessageBodyTypes:
    NODE_ID = 83
    FAVOR = 84
    VALID_THRU = 85
    EXPIRED_NODE_ID = 86

class ExpireMessageBodyTlv(TlvModel):
    node_id = BytesField(ExpireMessageBodyTypes.NODE_ID)
    favor = UintField(ExpireMessageBodyTypes.FAVOR)
    valid_thru = UintField(ExpireMessageBodyTypes.VALID_THRU)
    expired_node_id = BytesField(ExpireMessageBodyTypes.EXPIRED_NODE_ID)

class ExpireMessageBody(MessageBodyBase):
    def __init__(self, nid:str, seq:int, raw_bytes:bytes):
        super(ExpireMessageBody, self).__init__(nid, seq)
        self.expire_message_body = ExpireMessageBodyTlv.parse(raw_bytes)

    async def apply(self, global_view):
        #TODO: apply this add msg to the global view
        return
