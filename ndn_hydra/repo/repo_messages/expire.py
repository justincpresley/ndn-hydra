# ----------------------------------------------------------
# NDN Hydra Expire Group Message
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
import time
from ndn_hydra.repo.repo_messages.message_base import MessageBodyBase
from ndn_hydra.repo.repo_messages.store import StoreMessageBodyTlv
from ndn_hydra.repo.global_view.global_view import GlobalView

class ExpireMessageBodyTypes:
    SESSION_ID = 83
    NODE_NAME = 84
    EXPIRE_AT = 85
    FAVOR = 86

    EXPIRED_SESSION_ID = 90

class ExpireMessageBodyTlv(TlvModel):
    session_id = BytesField(ExpireMessageBodyTypes.SESSION_ID)
    node_name = BytesField(ExpireMessageBodyTypes.NODE_NAME)
    expire_at = UintField(ExpireMessageBodyTypes.EXPIRE_AT)
    favor = BytesField(ExpireMessageBodyTypes.FAVOR)
    expired_session_id = BytesField(ExpireMessageBodyTypes.EXPIRED_SESSION_ID)

class ExpireMessageBody(MessageBodyBase):
    def __init__(self, nid:str, seq:int, raw_bytes:bytes):
        super(ExpireMessageBody, self).__init__(nid, seq)
        self.message_body = ExpireMessageBodyTlv.parse(raw_bytes)

    async def apply(self, global_view: GlobalView, fetch_file: Callable, svs, config):
        session_id = self.message_body.session_id.tobytes().decode()
        node_name = self.message_body.node_name.tobytes().decode()
        expire_at = self.message_body.expire_at
        favor = float(self.message_body.favor.tobytes().decode())
        expired_session_id = self.message_body.expired_session_id.tobytes().decode()
        val = "[MSG][EXPIRE]  sid={sid};exp_sid={esid}".format(
            sid=session_id,
            esid=expired_session_id
        )
        self.logger.info(val)
        global_view.expire_session(expired_session_id)
        # am I at the top of any insertion's backup list?
        underreplicated_insertions = global_view.get_underreplicated_insertions()
        from .message import MessageTlv, MessageTypes
        for underreplicated_insertion in underreplicated_insertions:
            deficit = underreplicated_insertion['desired_copies'] - len(underreplicated_insertion['stored_bys'])
            for backuped_by in underreplicated_insertion['backuped_bys']:
                if (backuped_by['session_id'] == config['session_id']) and (backuped_by['rank'] < deficit):

                    digests = underreplicated_insertion['digests']
                    self.logger.debug(type(digests[0]))

                    fetch_file(underreplicated_insertion['id'], underreplicated_insertion['file_name'], underreplicated_insertion['packets'], underreplicated_insertion['digests'], underreplicated_insertion['fetch_path'])

                    # # generate store msg and send
                    # # store tlv
                    # expire_at = int(time.time()+(config['period']*2))
                    # favor = 1.85
                    # store_message_body = StoreMessageBodyTlv()
                    # store_message_body.session_id = config['session_id'].encode()
                    # store_message_body.node_name = config['node_name'].encode()
                    # store_message_body.expire_at = expire_at
                    # store_message_body.favor = str(favor).encode()
                    # store_message_body.insertion_id = underreplicated_insertion['id'].encode()
                    # # store msg
                    # store_message = MessageTlv()
                    # store_message.header = MessageTypes.STORE
                    # store_message.body = store_message_body.encode()
                    # # apply globalview and send msg thru SVS
                    # # next_state_vector = svs.getCore().getStateVector().get(config['session_id']) + 1
                    # global_view.store_file(underreplicated_insertion['id'], config['session_id'])
                    # svs.publishData(store_message.encode())
                    # val = "[MSG][STORE]+  sid={sid};iid={iid}".format(
                    #     sid=config['session_id'],
                    #     iid=underreplicated_insertion['id']
                    # )
                    # self.logger.info(val)
        # update session
        global_view.update_session(session_id, node_name, expire_at, favor, self.seq)
        return