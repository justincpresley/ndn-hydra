# ----------------------------------------------------------
# NDN Hydra Remove Group Message
# ----------------------------------------------------------
# @Project: NDN Hydra
# @Date:    2021-01-25
# @Author:  Zixuan Zhong
# @Author:  Justin C Presley
# @Author:  Daniel Achee
# @Source-Code: https://github.com/UCLA-IRL/ndn-hydra
# @Pip-Library: https://pypi.org/project/ndn-hydra/
# ----------------------------------------------------------

from typing import Callable
from ndn.encoding import *
from ndn_hydra.repo.global_view.global_view import GlobalView
from ndn_hydra.repo.repo_messages.message_base import MessageBodyBase

class RemoveMessageBodyTypes:
    SESSION_ID = 83
    NODE_NAME = 84
    EXPIRE_AT = 85
    FAVOR = 86

    INSERTION_ID = 90

class RemoveMessageBodyTlv(TlvModel):
    session_id = BytesField(RemoveMessageBodyTypes.SESSION_ID)
    node_name = BytesField(RemoveMessageBodyTypes.NODE_NAME)
    expire_at = UintField(RemoveMessageBodyTypes.EXPIRE_AT)
    favor = BytesField(RemoveMessageBodyTypes.FAVOR)
    insertion_id = BytesField(RemoveMessageBodyTypes.INSERTION_ID)

class RemoveMessageBody(MessageBodyBase):
    def __init__(self, nid:str, seq:int, raw_bytes:bytes):
        super(RemoveMessageBody, self).__init__(nid, seq)
        self.message_body = RemoveMessageBodyTlv.parse(raw_bytes)

    async def apply(self, global_view: GlobalView, fetch_file: Callable, svs, config):
        session_id = self.message_body.session_id.tobytes().decode()
        node_name = self.message_body.node_name.tobytes().decode()
        expire_at = self.message_body.expire_at
        favor = float(self.message_body.favor.tobytes().decode())
        insertion_id = self.message_body.insertion_id.tobytes().decode()
        val = "[MSG][REMOVE]  iid={iid}".format(
            sid=session_id,
            iid=insertion_id
        )
        self.logger.info(val)
        # if insertion
        insertion = global_view.get_insertion(insertion_id)
        if (insertion == None) or (insertion['is_deleted'] == True):
            # add store to pending_stores
            self.logger.warning('nothing to remove')
        else:
            global_view.delete_insertion(insertion_id)
            # TODO: remove from data_storage
        # update session
        global_view.update_session(session_id, node_name, expire_at, favor, self.seq)
        return