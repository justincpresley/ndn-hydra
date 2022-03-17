# -------------------------------------------------------------
# NDN Hydra Add Group Message
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
from ndn_hydra.repo.modules.global_view import GlobalView
from ndn_hydra.repo.group_messages.specific_message import SpecificMessage
from ndn_hydra.repo.protocol.base_models import File

class AddMessageTypes:
    NODE_NAME = 84
    FAVOR = 86

    FILE = 91
    DESIRED_COPIES = 92
    FETCH_PATH = 93
    IS_STORED_BY_ORIGIN = 94

    BACKUP = 100
    BACKUP_NODE_NAME = 101
    BACKUP_NONCE = 102

class FetchPathTlv(TlvModel):
    prefix = NameField()

class BackupTlv(TlvModel):
    node_name = BytesField(AddMessageTypes.BACKUP_NODE_NAME)
    nonce = BytesField(AddMessageTypes.BACKUP_NONCE)

class AddMessageTlv(TlvModel):
    node_name = BytesField(AddMessageTypes.NODE_NAME)
    favor = BytesField(AddMessageTypes.FAVOR)
    file = ModelField(AddMessageTypes.FILE, File)

    desired_copies = UintField(AddMessageTypes.DESIRED_COPIES)
    fetch_path = ModelField(AddMessageTypes.FETCH_PATH, FetchPathTlv)
    is_stored_by_origin = UintField(AddMessageTypes.IS_STORED_BY_ORIGIN)
    backup_list = RepeatedField(ModelField(AddMessageTypes.BACKUP, BackupTlv))

class AddMessage(SpecificMessage):
    def __init__(self, nid:str, seqno:int, raw_bytes:bytes):
        super(AddMessage, self).__init__(nid, seqno)
        self.message = AddMessageTlv.parse(raw_bytes)

    async def apply(self, global_view: GlobalView, fetch_file: Callable, svs, config):
        node_name = self.message.node_name.tobytes().decode()
        favor = float(self.message.favor.tobytes().decode())
        file = self.message.file
        file_name = Name.to_str(file.file_name)
        packets = file.packets
        digests = file.digests
        size = file.size
        desired_copies = self.message.desired_copies
        fetch_path = self.message.fetch_path.prefix
        is_stored_by_origin = False if (self.message.is_stored_by_origin == 0) else True
        backups = self.message.backup_list
        backup_list = []
        bak = ""
        for backup in backups:
            backup_list.append((backup.node_name.tobytes().decode(), backup.nonce.tobytes().decode()))
            bak = bak + backup.node_name.tobytes().decode() + ","
        self.logger.info(f"[MSG][ADD]     nam={node_name};fil={file_name};cop={desired_copies};pck={packets};siz={size};slf={1 if is_stored_by_origin else 0};bak={bak}")
        global_view.add_file(
            file_name,
            size,
            node_name,
            Name.to_str(fetch_path),
            self.seqno,
            b''.join(digests),
            packets=packets,
            desired_copies=desired_copies
        )

        global_view.set_backups(file_name, backup_list)

        # get pending stores
        copies_needed = desired_copies
        pending_stores = global_view.get_pending_stores(file_name)
        for pending_store in pending_stores:
            # data_storage.add_metainfos(insertion_id, Name.to_str(file_name), packets, digests, Name.to_str(fetch_path))
            global_view.store_file(file_name, pending_store)
            copies_needed -= 1

        # if I need to store this file
        # if is_stored_by_origin:
        #     copies_needed -= 1
        need_to_store = False
        for i in range(copies_needed):
            backup = backup_list[i]
            if backup[0] == config['node_name']:
                need_to_store = True
                break
        if need_to_store == True:
            fetch_file(file_name, packets, digests, Name.to_str(fetch_path))

            # from .message import MessageTlv, MessageTypes
            # # generate store msg and send
            # # store tlv
            # expire_at = int(time.time()+(config['period']*2))
            # favor = 1.85
            # store_message = StoreMessageTlv()
            # store_message.session_id = config['session_id'].encode()
            # store_message.node_name = config['node_name'].encode()
            # store_message.expire_at = expire_at
            # store_message.favor = str(favor).encode()
            # store_message.insertion_id = insertion_id.encode()
            # # store msg
            # store_message = MessageTlv()
            # store_message.type = MessageTypes.STORE
            # store_message.value = store_message.encode()
            # # apply globalview and send msg thru SVS
            # # next_state_vector = svs.getCore().getStateVector().get(config['session_id']) + 1

            # # global_view.store_file(insertion_id, config['session_id'])
            # svs.publishData(store_message.encode())
            # val = "[MSG][STORE]*  sid={sid};iid={iid}".format(
            #     sid=config['session_id'],
            #     iid=insertion_id
            # )
            # self.logger.info(val)
        # update session
        global_view.update_node(node_name, favor, self.seqno)