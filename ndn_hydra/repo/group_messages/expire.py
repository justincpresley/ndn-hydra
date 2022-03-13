# -------------------------------------------------------------
# NDN Hydra Expire Group Message
# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Date:    2021-01-25
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/justincpresley/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

from typing import Callable
from ndn.encoding import *
import time
from ndn_hydra.repo.group_messages.specific_message import SpecificMessage
from ndn_hydra.repo.global_view.global_view import GlobalView

class ExpireMessageTypes:
    SESSION_ID = 83
    NODE_NAME = 84
    EXPIRE_AT = 85
    FAVOR = 86

    EXPIRED_SESSION_ID = 90

class ExpireMessageTlv(TlvModel):
    session_id = BytesField(ExpireMessageTypes.SESSION_ID)
    node_name = BytesField(ExpireMessageTypes.NODE_NAME)
    expire_at = UintField(ExpireMessageTypes.EXPIRE_AT)
    favor = BytesField(ExpireMessageTypes.FAVOR)
    expired_session_id = BytesField(ExpireMessageTypes.EXPIRED_SESSION_ID)

class ExpireMessage(SpecificMessage):
    def __init__(self, nid:str, seqno:int, raw_bytes:bytes):
        super(ExpireMessage, self).__init__(nid, seqno)
        self.message = ExpireMessageTlv.parse(raw_bytes)

    async def apply(self, global_view: GlobalView, fetch_file: Callable, svs, config):
        session_id = self.message.session_id.tobytes().decode()
        node_name = self.message.node_name.tobytes().decode()
        expire_at = self.message.expire_at
        favor = float(self.message.favor.tobytes().decode())
        expired_session_id = self.message.expired_session_id.tobytes().decode()
        val = "[MSG][EXPIRE]  sid={sid};exp_sid={esid}".format(
            sid=session_id,
            esid=expired_session_id
        )
        self.logger.info(val)
        global_view.expire_session(expired_session_id)
        # am I at the top of any insertion's backup list?
        underreplicated_insertions = global_view.get_underreplicated_insertions()

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
                    # store_message = StoreMessageTlv()
                    # store_message.session_id = config['session_id'].encode()
                    # store_message.node_name = config['node_name'].encode()
                    # store_message.expire_at = expire_at
                    # store_message.favor = str(favor).encode()
                    # store_message.insertion_id = underreplicated_insertion['id'].encode()
                    # # store msg
                    # store_message = MessageTlv()
                    # store_message.type = MessageTypes.STORE
                    # store_message.value = store_message.encode()
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
        global_view.update_session(session_id, node_name, expire_at, favor, self.seqno)
        return