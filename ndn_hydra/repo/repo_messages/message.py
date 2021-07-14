# ----------------------------------------------------------
# NDN Hydra General Group Message
# ----------------------------------------------------------
# @Project: NDN Hydra
# @Date:    2021-01-25
# @Author:  Zixuan Zhong
# @Author:  Justin C Presley
# @Author:  Daniel Achee
# @Source-Code: https://github.com/UCLA-IRL/ndn-hydra
# @Pip-Library: https://pypi.org/project/ndn-hydra/
# ----------------------------------------------------------

from ndn.encoding import *
from ndn_hydra.repo.repo_messages.add import AddMessageBody
from ndn_hydra.repo.repo_messages.remove import RemoveMessageBody
from ndn_hydra.repo.repo_messages.store import StoreMessageBody
from ndn_hydra.repo.repo_messages.claim import ClaimMessageBody
from ndn_hydra.repo.repo_messages.heartbeat import HeartbeatMessageBody
from ndn_hydra.repo.repo_messages.expire import ExpireMessageBody

class MessageTypes:
    ADD = 1
    REMOVE = 2
    STORE = 3
    CLAIM = 4
    HEARTBEAT = 5
    EXPIRE = 6

class MessageTlv(TlvModel):
    header = UintField(0x80)
    body = BytesField(0x81)

class Message:
    def __init__(self, nid:str, seq:int, raw_bytes:bytes):
        self.nid = nid
        self.seq = seq
        self.message = MessageTlv.parse(raw_bytes)

    def get_message_header(self):
        return self.message.header

    def get_message_body(self):
        message_type = self.message.header
        raw_bytes = self.message.body.tobytes()
        if message_type == MessageTypes.ADD:
            return AddMessageBody(self.nid, self.seq, raw_bytes)
        elif message_type == MessageTypes.REMOVE:
            return RemoveMessageBody(self.nid, self.seq, raw_bytes)
        elif message_type == MessageTypes.STORE:
            return StoreMessageBody(self.nid, self.seq, raw_bytes)
        elif message_type == MessageTypes.CLAIM:
            return ClaimMessageBody(self.nid, self.seq, raw_bytes)
        elif message_type == MessageTypes.HEARTBEAT:
            return HeartbeatMessageBody(self.nid, self.seq, raw_bytes)
        elif message_type == MessageTypes.EXPIRE:
            return ExpireMessageBody(self.nid, self.seq, raw_bytes)
        else:
            return None


