# ----------------------------------------------------------
# NDN Hydra Add Group Message
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
import json
import time
from ndn_hydra.repo.global_view.global_view import GlobalView
from ndn_hydra.repo.repo_messages.message_base import MessageBodyBase
from ndn_hydra.repo.repo_messages.store import StoreMessageBodyTlv

class AddMessageBodyTypes:
    SESSION_ID = 83
    NODE_NAME = 84
    EXPIRE_AT = 85
    FAVOR = 86

    INSERTION_ID = 90
    FILE = 91
    DESIRED_COPIES = 92
    PACKETS = 93
    DIGEST = 94
    SIZE = 95
    SEQUENCE_NUMBER = 96
    FETCH_PATH = 97
    IS_STORED_BY_ORIGIN = 98

    BACKUP = 100
    BACKUP_SESSION_ID = 101
    BACKUP_NONCE = 102

class FileTlv(TlvModel):
    file_name = NameField()
    desired_copies = UintField(AddMessageBodyTypes.DESIRED_COPIES, default=3)
    packets = UintField(AddMessageBodyTypes.PACKETS)
    digests = RepeatedField(BytesField(AddMessageBodyTypes.DIGEST))
    size = UintField(AddMessageBodyTypes.SIZE)

class FetchPathTlv(TlvModel):
    prefix = NameField()

class BackupTlv(TlvModel):
    session_id = BytesField(AddMessageBodyTypes.BACKUP_SESSION_ID)
    nonce = BytesField(AddMessageBodyTypes.BACKUP_NONCE)

class AddMessageBodyTlv(TlvModel):
    session_id = BytesField(AddMessageBodyTypes.SESSION_ID)
    node_name = BytesField(AddMessageBodyTypes.NODE_NAME)
    expire_at = UintField(AddMessageBodyTypes.EXPIRE_AT)
    favor = BytesField(AddMessageBodyTypes.FAVOR)
    insertion_id = BytesField(AddMessageBodyTypes.INSERTION_ID)
    file = ModelField(AddMessageBodyTypes.FILE, FileTlv)

    sequence_number = UintField(AddMessageBodyTypes.SEQUENCE_NUMBER)
    fetch_path = ModelField(AddMessageBodyTypes.FETCH_PATH, FetchPathTlv)
    is_stored_by_origin = UintField(AddMessageBodyTypes.IS_STORED_BY_ORIGIN)
    backup_list = RepeatedField(ModelField(AddMessageBodyTypes.BACKUP, BackupTlv))

class AddMessageBody(MessageBodyBase):
    def __init__(self, nid:str, seq:int, raw_bytes:bytes):
        super(AddMessageBody, self).__init__(nid, seq)
        self.message_body = AddMessageBodyTlv.parse(raw_bytes)

    async def apply(self, global_view: GlobalView, fetch_file: Callable, svs, config):
        session_id = self.message_body.session_id.tobytes().decode()
        node_name = self.message_body.node_name.tobytes().decode()
        expire_at = self.message_body.expire_at
        favor = float(self.message_body.favor.tobytes().decode())
        insertion_id = self.message_body.insertion_id.tobytes().decode()
        file = self.message_body.file
        file_name = file.file_name
        desired_copies = file.desired_copies
        packets = file.packets
        digests = file.digests
        size = file.size
        sequence_number = self.message_body.sequence_number
        fetch_path = self.message_body.fetch_path.prefix
        is_stored_by_origin = False if (self.message_body.is_stored_by_origin == 0) else True
        backups = self.message_body.backup_list
        backup_list = []
        bak = ""
        for backup in backups:
            backup_list.append((backup.session_id.tobytes().decode(), backup.nonce.tobytes().decode()))
            bak = bak + backup.session_id.tobytes().decode() + ","
        val = "[MSG][ADD]     sid={sid};iid={iid};file={fil};cop={cop};pck={pck};siz={siz};seq={seq};slf={slf};bak={bak}".format(
            sid=session_id,
            iid=insertion_id,
            fil=Name.to_str(file_name),
            cop=desired_copies,
            pck=packets,
            siz=size,
            seq=sequence_number,
            slf=1 if is_stored_by_origin else 0,
            bak=bak
        )
        self.logger.info(val)
        global_view.add_insertion(
            insertion_id,
            Name.to_str(file_name),
            sequence_number,
            size,
            session_id,
            Name.to_str(fetch_path),
            self.seq,
            b''.join(digests),
            packets=packets,
            desired_copies=desired_copies
        )

        global_view.set_backups(insertion_id, backup_list)

        # get pending stores
        copies_needed = desired_copies
        pending_stores = global_view.get_pending_stores(insertion_id)
        for pending_store in pending_stores:
            # data_storage.add_metainfos(insertion_id, Name.to_str(file_name), packets, digests, Name.to_str(fetch_path))
            global_view.store_file(insertion_id, pending_store)
            copies_needed -= 1

        # if I need to store this file
        # if is_stored_by_origin:
        #     copies_needed -= 1
        need_to_store = False
        for i in range(copies_needed):
            backup = backup_list[i]
            if backup[0] == config['session_id']:
                need_to_store = True
                break
        if need_to_store == True:
            fetch_file(insertion_id, Name.to_str(file_name), packets, digests, Name.to_str(fetch_path))

            # from .message import MessageTlv, MessageTypes
            # # generate store msg and send
            # # store tlv
            # expire_at = int(time.time()+(config['period']*2))
            # favor = 1.85
            # store_message_body = StoreMessageBodyTlv()
            # store_message_body.session_id = config['session_id'].encode()
            # store_message_body.node_name = config['node_name'].encode()
            # store_message_body.expire_at = expire_at
            # store_message_body.favor = str(favor).encode()
            # store_message_body.insertion_id = insertion_id.encode()
            # # store msg
            # store_message = MessageTlv()
            # store_message.header = MessageTypes.STORE
            # store_message.body = store_message_body.encode()
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
        global_view.update_session(session_id, node_name, expire_at, favor, self.seq)
        return
