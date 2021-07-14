# ----------------------------------------------------------
# NDN Hydra Claim Group Message
# ----------------------------------------------------------
# @Project: NDN Hydra
# @Date:    2021-01-25
# @Author:  Zixuan Zhong
# @Author:  Justin C Presley
# @Author:  Daniel Achee
# @Source-Code: https://github.com/UCLA-IRL/ndn-hydra
# @Pip-Library: https://pypi.org/project/ndn-hydra/
# ----------------------------------------------------------

import copy
from typing import Callable
import time
from ndn.encoding import *
from ndn_hydra.repo.global_view.global_view import GlobalView
from ndn_hydra.repo.repo_messages.message_base import MessageBodyBase

class ClaimMessageTypes:
    REQUEST = 1
    COMMITMENT = 2

class ClaimMessageBodyTypes:
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


class ClaimMessageBodyTlv(TlvModel):
    session_id = BytesField(ClaimMessageBodyTypes.SESSION_ID)
    node_name = BytesField(ClaimMessageBodyTypes.NODE_NAME)
    expire_at = UintField(ClaimMessageBodyTypes.EXPIRE_AT)
    favor = BytesField(ClaimMessageBodyTypes.FAVOR)
    insertion_id = BytesField(ClaimMessageBodyTypes.INSERTION_ID)
    type = UintField(ClaimMessageBodyTypes.TYPE)
    claimer_session_id = BytesField(ClaimMessageBodyTypes.CLAIMER_SESSION_ID)
    claimer_nonce = BytesField(ClaimMessageBodyTypes.CLAIMER_NONCE)
    authorizer_session_id = BytesField(ClaimMessageBodyTypes.AUTHORIZER_SESSION_ID)
    authorizer_nonce = BytesField(ClaimMessageBodyTypes.AUTHORIZER_NONCE)

class ClaimMessageBody(MessageBodyBase):
    def __init__(self, nid:str, seq:int, raw_bytes:bytes):
        super(ClaimMessageBody, self).__init__(nid, seq)
        self.message_body = ClaimMessageBodyTlv.parse(raw_bytes)

    async def apply(self, global_view: GlobalView, fetch_file: Callable, svs, config):
        session_id = self.message_body.session_id.tobytes().decode()
        node_name = self.message_body.node_name.tobytes().decode()
        expire_at = self.message_body.expire_at
        favor = float(self.message_body.favor.tobytes().decode())
        insertion_id = self.message_body.insertion_id.tobytes().decode()
        type = self.message_body.type
        claimer_session_id = self.message_body.claimer_session_id.tobytes().decode()
        claimer_nonce = self.message_body.claimer_nonce.tobytes().decode()
        authorizer_session_id = self.message_body.authorizer_session_id.tobytes().decode()
        authorizer_nonce = self.message_body.authorizer_nonce.tobytes().decode()
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
                    claim_message_body = copy.copy(self.message_body)
                    claim_message_body.session_id = config['session_id'].encode()
                    claim_message_body.node_name = config['node_name'].encode()
                    claim_message_body.expire_at = expire_at
                    claim_message_body.favor = str(favor).encode()
                    claim_message_body.type = ClaimMessageTypes.COMMITMENT
                    # claim msg
                    claim_message = MessageTlv()
                    claim_message.header = MessageTypes.CLAIM
                    claim_message.body = claim_message_body.encode()
                    svs.publishData(claim_message.encode())
                    val = "[MSG][CLAIM.C]*sid={sid};iid={iid}".format(
                        sid=claimer_session_id,
                        iid=insertion_id
                    )
                    self.logger.info(val)
        # update session
        global_view.update_session(session_id, node_name, expire_at, favor, self.seq)
        return