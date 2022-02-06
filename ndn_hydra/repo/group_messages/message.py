# -------------------------------------------------------------
# NDN Hydra General Group Message
# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Date:    2021-01-25
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/justincpresley/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

from __future__ import annotations

from ndn.encoding import *
from typing import Optional
from ndn_hydra.repo.group_messages.specific_message import SpecificMessage
from ndn_hydra.repo.group_messages.add import AddMessageBody
from ndn_hydra.repo.group_messages.remove import RemoveMessageBody
from ndn_hydra.repo.group_messages.store import StoreMessageBody
from ndn_hydra.repo.group_messages.claim import ClaimMessageBody
from ndn_hydra.repo.group_messages.heartbeat import HeartbeatMessageBody
from ndn_hydra.repo.group_messages.expire import ExpireMessageBody

class MessageTypes:
    ADD = 1
    REMOVE = 2
    STORE = 3
    CLAIM = 4
    HEARTBEAT = 5
    EXPIRE = 6

class Message(TlvModel):
    type = UintField(0x80)
    value = BytesField(0x81)
    @staticmethod
    def specify(nid:str, seqno:int, message_bytes:bytes) -> Optional[SpecificMessage]:
        message = Message.parse(message_bytes)
        message_type, message_bytes = message.type, bytes(message.value)
        if message_type == MessageTypes.ADD:
            return AddMessageBody(nid, seqno, message_bytes)
        elif message_type == MessageTypes.REMOVE:
            return RemoveMessageBody(nid, seqno, message_bytes)
        elif message_type == MessageTypes.STORE:
            return StoreMessageBody(nid, seqno, message_bytes)
        elif message_type == MessageTypes.CLAIM:
            return ClaimMessageBody(nid, seqno, message_bytes)
        elif message_type == MessageTypes.HEARTBEAT:
            return HeartbeatMessageBody(nid, seqno, message_bytes)
        elif message_type == MessageTypes.EXPIRE:
            return ExpireMessageBody(nid, seqno, message_bytes)
        else:
            return None