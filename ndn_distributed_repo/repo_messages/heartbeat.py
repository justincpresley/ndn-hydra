from ndn.encoding import *
from .message_base import MessageBodyBase

class HeartbeatMessageBodyTypes:
    NODE_ID = 83
    FAVOR = 84
    VALID_THRU = 85

class HeartbeatMessageBodyTlv(TlvModel):
    node_id = BytesField(HeartbeatMessageBodyTypes.NODE_ID)
    favor = UintField(HeartbeatMessageBodyTypes.FAVOR)
    valid_thru = UintField(HeartbeatMessageBodyTypes.VALID_THRU)

class HeartbeatMessageBody(MessageBodyBase):
    def __init__(self, nid:str, seq:int, raw_bytes:bytes):
        super(HeartbeatMessageBody, self).__init__(nid, seq)
        self.heartbeat_message_body = HeartbeatMessageBodyTlv.parse(raw_bytes)

    async def apply(self, global_view):
        #TODO: apply this add msg to the global view
        return
