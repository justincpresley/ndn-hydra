# -------------------------------------------------------------
# NDN Hydra Claim Group Message
# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Date:    2021-01-25
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/justincpresley/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

import copy
from typing import Callable
import time
from ndn.encoding import *
from ndn_hydra.repo.global_view.global_view import GlobalView
from ndn_hydra.repo.group_messages.specific_message import SpecificMessage

class ClaimTypes:
    REQUEST = 1
    COMMITMENT = 2

class ClaimMessageTypes:
    SESSION_ID = 83
    NODE_NAME = 84
    EXPIRE_AT = 85
    FAVOR = 86

    INSERTION_ID = 90
    TYPE = 91 # 1=request; 2=commitment
    CLAIMER_SESSION_ID = 92
    CLAIMER_NONCE = 93
    AUTHORIZER_SESSION_ID = 94
    AUTHORIZER_NONCE = 95


class ClaimMessageTlv(TlvModel):
    session_id = BytesField(ClaimMessageTypes.SESSION_ID)
    node_name = BytesField(ClaimMessageTypes.NODE_NAME)
    expire_at = UintField(ClaimMessageTypes.EXPIRE_AT)
    favor = BytesField(ClaimMessageTypes.FAVOR)
    insertion_id = BytesField(ClaimMessageTypes.INSERTION_ID)
    type = UintField(ClaimMessageTypes.TYPE)
    claimer_session_id = BytesField(ClaimMessageTypes.CLAIMER_SESSION_ID)
    claimer_nonce = BytesField(ClaimMessageTypes.CLAIMER_NONCE)
    authorizer_session_id = BytesField(ClaimMessageTypes.AUTHORIZER_SESSION_ID)
    authorizer_nonce = BytesField(ClaimMessageTypes.AUTHORIZER_NONCE)

class ClaimMessage(SpecificMessage):
    def __init__(self, nid:str, seqno:int, raw_bytes:bytes):
        super(ClaimMessage, self).__init__(nid, seqno)
        self.message = ClaimMessageTlv.parse(raw_bytes)

    async def apply(self, global_view: GlobalView, fetch_file: Callable, svs, config):
        session_id = self.message.session_id.tobytes().decode()
        node_name = self.message.node_name.tobytes().decode()
        expire_at = self.message.expire_at
        favor = float(self.message.favor.tobytes().decode())
        insertion_id = self.message.insertion_id.tobytes().decode()
        type = self.message.type
        claimer_session_id = self.message.claimer_session_id.tobytes().decode()
        claimer_nonce = self.message.claimer_nonce.tobytes().decode()
        authorizer_session_id = self.message.authorizer_session_id.tobytes().decode()
        authorizer_nonce = self.message.authorizer_nonce.tobytes().decode()
        insertion = global_view.get_insertion(insertion_id)
        if type == ClaimMessageTypes.COMMITMENT:
            rank = len(insertion['backuped_bys'])
            val = "[MSG][CLAIM.C] sid={sid};iid={iid}".format(
                sid=claimer_session_id,
                iid=insertion_id
            )
            self.logger.info(val)
            global_view.add_backup(insertion_id, claimer_session_id, rank, claimer_nonce)
        else:
            val = "[MSG][CLAIM.R] sid={sid};iid={iid}".format(
                sid=claimer_session_id,
                iid=insertion_id
            )
            self.logger.info(val)
            if authorizer_session_id == config['session_id']:
                from .message import MessageTlv, MessageTypes
                commit = False
                if (len(insertion['backuped_bys']) == 0) and (insertion['stored_bys'][-1] == config['session_id']) and (authorizer_nonce == insertion['id']):
                    global_view.add_backup(insertion_id, claimer_session_id, 0, claimer_nonce)
                    commit = True
                if (len(insertion['backuped_bys']) > 0) and (insertion['backuped_bys'][-1]['session_id'] == config['session_id']) and (authorizer_nonce == insertion['backuped_bys'][-1]['nonce']):
                    global_view.add_backup(insertion_id, claimer_session_id, len(insertion['backuped_bys']), claimer_nonce)
                    commit = True
                if commit == True:
                    # claim tlv
                    expire_at = int(time.time()+(config['period']*2))
                    favor = 1.85
                    claim_message = copy.copy(self.message)
                    claim_message.session_id = config['session_id'].encode()
                    claim_message.node_name = config['node_name'].encode()
                    claim_message.expire_at = expire_at
                    claim_message.favor = str(favor).encode()
                    claim_message.type = ClaimTypes.COMMITMENT
                    # claim msg
                    claim_message = MessageTlv()
                    claim_message.type = MessageTypes.CLAIM
                    claim_message.value = claim_message.encode()
                    svs.publishData(claim_message.encode())
                    val = "[MSG][CLAIM.C]*sid={sid};iid={iid}".format(
                        sid=claimer_session_id,
                        iid=insertion_id
                    )
                    self.logger.info(val)
        # update session
        global_view.update_session(session_id, node_name, expire_at, favor, self.seqno)
        return
