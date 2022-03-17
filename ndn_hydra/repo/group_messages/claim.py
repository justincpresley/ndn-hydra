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
from ndn_hydra.repo.modules.global_view import GlobalView
from ndn_hydra.repo.group_messages.specific_message import SpecificMessage

class ClaimTypes:
    REQUEST = 1
    COMMITMENT = 2
class ClaimMessageTypes:
    NODE_NAME = 84
    FAVOR = 86

    TYPE = 91 # 1=request; 2=commitment
    CLAIMER_NODE_NAME = 92
    CLAIMER_NONCE = 93
    AUTHORIZER_NODE_NAME = 94
    AUTHORIZER_NONCE = 95


class ClaimMessageTlv(TlvModel):
    node_name = BytesField(ClaimMessageTypes.NODE_NAME)
    favor = BytesField(ClaimMessageTypes.FAVOR)
    file_name = NameField()
    type = UintField(ClaimMessageTypes.TYPE)
    claimer_node_name = BytesField(ClaimMessageTypes.CLAIMER_NODE_NAME)
    claimer_nonce = BytesField(ClaimMessageTypes.CLAIMER_NONCE)
    authorizer_node_name = BytesField(ClaimMessageTypes.AUTHORIZER_NODE_NAME)
    authorizer_nonce = BytesField(ClaimMessageTypes.AUTHORIZER_NONCE)

class ClaimMessage(SpecificMessage):
    def __init__(self, nid:str, seqno:int, raw_bytes:bytes):
        super(ClaimMessage, self).__init__(nid, seqno)
        self.message = ClaimMessageTlv.parse(raw_bytes)

    async def apply(self, global_view, fetch_file, svs, config):
        node_name = self.message.node_name.tobytes().decode()
        favor = float(self.message.favor.tobytes().decode())
        file_name = Name.to_str(self.message.file_name)
        claimer_node_name = self.message.claimer_node_name.tobytes().decode()
        claimer_nonce = self.message.claimer_nonce.tobytes().decode()
        authorizer_node_name = self.message.authorizer_node_name.tobytes().decode()
        authorizer_nonce = self.message.authorizer_nonce.tobytes().decode()
        file = global_view.get_file(file_name)
        if self.message.type == ClaimTypes.COMMITMENT:
            rank = len(file['backuped_bys'])
            self.logger.info(f"[MSG][CLAIM.C]  nam={claimer_node_name};fil={file_name}")
            global_view.add_backup(file_name, claimer_node_name, rank, claimer_nonce)
        else:
            self.logger.info(f"[MSG][CLAIM.R]  nam={claimer_node_name};fil={file_name}")
            if authorizer_node_name == config['node_name']:
                from .message import MessageTlv, MessageTypes
                commit = False
                if (len(file['backuped_bys']) == 0) and (file['stored_bys'][-1] == config['node_name']) and (authorizer_nonce == file['id']):
                    global_view.add_backup(file_name, claimer_node_name, 0, claimer_nonce)
                    commit = True
                if (len(file['backuped_bys']) > 0) and (file['backuped_bys'][-1]['node_name'] == config['node_name']) and (authorizer_nonce == file['backuped_bys'][-1]['nonce']):
                    global_view.add_backup(file_name, claimer_node_name, len(file['backuped_bys']), claimer_nonce)
                    commit = True
                if commit == True:
                    # claim tlv
                    favor = 1.85
                    claim_message = copy.copy(self.message)
                    claim_message.node_name = config['node_name'].encode()
                    claim_message.favor = str(favor).encode()
                    claim_message.type = ClaimTypes.COMMITMENT
                    # claim msg
                    claim_message = MessageTlv()
                    claim_message.type = MessageTypes.CLAIM
                    claim_message.value = claim_message.encode()
                    svs.publishData(claim_message.encode())
                    self.logger.info(f"[MSG][CLAIM.C]* nam={claimer_node_name};fil={file_name}")
        global_view.update_node(node_name, favor, self.seqno)