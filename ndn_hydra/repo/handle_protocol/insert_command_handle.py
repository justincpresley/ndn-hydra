# ----------------------------------------------------------
# NDN Hydra Insert Command Handle
# ----------------------------------------------------------
# @Project: NDN Hydra
# @Date:    2021-01-25
# @Author:  Zixuan Zhong
# @Author:  Justin C Presley
# @Author:  Daniel Achee
# @Source-Code: https://github.com/UCLA-IRL/ndn-hydra
# @Pip-Library: https://pypi.org/project/ndn-hydra/
# ----------------------------------------------------------

import asyncio as aio
import logging
import random
import secrets
import time
from ndn.app import NDNApp
from ndn.encoding import Name, NonStrictName, Component, DecodeError
from ndn_python_repo import Storage
from ndn_hydra.repo.handle_protocol.protocol_handle_base import ProtocolHandle
from ndn_hydra.repo.protocol.repo_commands import RepoCommand
from ndn_hydra.repo.utils.pubsub import PubSub
from ndn_hydra.repo.global_view.global_view import GlobalView
from ndn_hydra.repo.repo_messages.add import FileTlv, FetchPathTlv, BackupTlv, AddMessageBodyTlv
from ndn_hydra.repo.repo_messages.message import MessageTlv, MessageTypes
from ndn_hydra.repo.main.main_loop import MainLoop

class InsertCommandHandle(ProtocolHandle):
    """
    InsertCommandHandle processes insert command handles, and deletes corresponding data stored
    in the database.
    """
    def __init__(self, app: NDNApp, data_storage: Storage, pb: PubSub, config: dict,
                main_loop: MainLoop, global_view: GlobalView):
        """
        :param app: NDNApp.
        :param data_storage: Storage.
        :param pb: PubSub.
        :param config: All config Info.
        :param main_loop: the Main Loop.
        :param global_view: Global View.
        """
        super(InsertCommandHandle, self).__init__(app, data_storage, pb, config)
        self.prefix = None
        self.main_loop = main_loop
        self.global_view = global_view

    async def listen(self, prefix: NonStrictName):
        """
        Register routes for command interests.
        This function needs to be called explicitly after initialization.
        :param name: NonStrictName. The name prefix to listen on.
        """
        self.prefix = prefix

        # subscribe to insert messages
        self.pb.subscribe(self.prefix + ['insert'], self._on_insert_msg)

        # start to announce process status
        # await self._schedule_announce_process_status(period=3)

    def _on_insert_msg(self, msg):
        try:
            cmd = RepoCommand.parse(msg)
            #if cmd.file == None:
            #    raise DecodeError()
        except (DecodeError, IndexError) as exc:
            logging.warning('Parameter interest decoding failed')
            return
        aio.ensure_future(self._process_insert(cmd))

    async def _process_insert(self, cmd: RepoCommand):
        """
        Process insert command.
        """
        # print("Process Insert Command for File: ")
        # print("receive INSERT command for file: {}".format(Name.to_str(cmd.file.file_name)))

        file_name = cmd.file.file_name
        desired_copies = cmd.file.desired_copies
        packets = cmd.file.packets
        digests = cmd.file.digests
        size = cmd.file.size
        sequence_number = cmd.sequence_number
        fetch_path = cmd.fetch_path.prefix

        self.logger.info("[cmd][INSERT] file {}".format(Name.to_str(file_name)))

        # TODO: check duplicate sequence number

        # are there enough sessions?
        if desired_copies == 0:
            self.logger.warning("desired_copies is 0")
            return

        sessions = self.global_view.get_sessions()

        if len(sessions) < (desired_copies * 2):
            self.logger.warning("not enough node sessions") # TODO: notify the client?
            return

        # generate unique insertion_id
        insertion_id = secrets.token_hex(8)
        while self.global_view.get_insertion(insertion_id) != None:
            insertion_id = secrets.token_hex(8)

        # select sessions
        random.shuffle(sessions)
        picked_sessions = random.sample(sessions, (desired_copies * 2))

        pickself = False
        for i in range(desired_copies):
            if picked_sessions[i]['id'] == self.config['session_id']:
                pickself = True
                break

        # if pickself:
        #     # TODO: fetch and store this file
        #     pass
            # print("pick myself")
            # picked_sessions = list(filter(lambda x: x['id'] != self.config['session_id'], picked_sessions))


        backups = []
        backup_list = []
        for picked_session in picked_sessions:
            session_id = picked_session['id']
            nonce = secrets.token_hex(4)
            backup = BackupTlv()
            backup.session_id = session_id.encode()
            backup.nonce = nonce.encode()
            backups.append(backup)
            backup_list.append((session_id, nonce))


        # add tlv
        expire_at = int(time.time()+(self.config['period']*2))
        favor = 1.85
        add_message_body = AddMessageBodyTlv()
        add_message_body.session_id = self.config['session_id'].encode()
        add_message_body.node_name = self.config['node_name'].encode()
        add_message_body.expire_at = expire_at
        add_message_body.favor = str(favor).encode()
        add_message_body.insertion_id = insertion_id.encode()
        add_message_body.file = FileTlv()
        add_message_body.file.file_name = file_name
        add_message_body.file.desired_copies = desired_copies
        add_message_body.file.packets = packets
        add_message_body.file.digests = digests
        add_message_body.file.size = size
        add_message_body.sequence_number = sequence_number
        add_message_body.fetch_path = FetchPathTlv()
        add_message_body.fetch_path.prefix = fetch_path
        # add_message_body.is_stored_by_origin = 1 if pickself else 0
        add_message_body.is_stored_by_origin = 0
        add_message_body.backup_list = backups
        # add msg
        add_message = MessageTlv()
        add_message.header = MessageTypes.ADD
        add_message.body = add_message_body.encode()
        # apply globalview and send msg thru SVS
        next_state_vector = self.main_loop.svs.getCore().getStateVector().get(self.config['session_id']) + 1
        self.global_view.add_insertion(
            insertion_id,
            Name.to_str(file_name),
            sequence_number,
            size,
            self.config['session_id'],
            Name.to_str(fetch_path),
            next_state_vector,
            b''.join(digests),
            packets=packets,
            desired_copies=desired_copies
        )
        if pickself:
            # self.global_view.store_file(insertion_id, self.config['session_id'])
            self.main_loop.fetch_file(insertion_id, Name.to_str(file_name), packets, digests, Name.to_str(fetch_path))
        self.global_view.set_backups(insertion_id, backup_list)
        self.main_loop.svs.publishData(add_message.encode())
        bak = ""
        for backup in backup_list:
            bak = bak + backup[0] + ","
        val = "[MSG][ADD]*    sid={sid};iid={iid};file={fil};cop={cop};pck={pck};siz={siz};seq={seq};slf={slf};bak={bak}".format(
            sid=self.config['session_id'],
            iid=insertion_id,
            fil=Name.to_str(file_name),
            cop=desired_copies,
            pck=packets,
            siz=size,
            seq=sequence_number,
            # slf=1 if pickself else 0,
            slf=0,
            bak=bak
        )
        self.logger.info(val)




