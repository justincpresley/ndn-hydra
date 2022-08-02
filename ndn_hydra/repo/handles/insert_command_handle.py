# -------------------------------------------------------------
# NDN Hydra Insert Command Handle
# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Date:    2021-01-25
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/justincpresley/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

import asyncio as aio
import logging
import random
import secrets
import time
from ndn.app import NDNApp
from ndn.encoding import Name, NonStrictName, Component, DecodeError
from ndn.storage import Storage
from ndn_hydra.repo.handles.protocol_handle_base import ProtocolHandle
from ndn_hydra.repo.protocol.base_models import InsertCommand, File
from ndn_hydra.repo.utils.pubsub import PubSub
from ndn_hydra.repo.modules.global_view import GlobalView
from ndn_hydra.repo.group_messages.add import FetchPathTlv, BackupTlv, AddMessageTlv
from ndn_hydra.repo.group_messages.message import Message, MessageTypes
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
        self.repo_prefix = config['repo_prefix']
        self.replication_degree = config['replication_degree']

    async def listen(self, prefix: NonStrictName):
        """
        Register routes for command interests.
        This function needs to be called explicitly after initialization.
        :param name: NonStrictName. The name prefix to listen on.
        """
        self.prefix = prefix
        self.logger.info(f'Insert handle: subscribing to {Name.to_str(self.prefix) + "/insert"}')
        self.pb.subscribe(self.prefix + ['insert'], self._on_insert_msg)
        # start to announce process status
        # await self._schedule_announce_process_status(period=3)

    def _on_insert_msg(self, msg):
        try:
            cmd = InsertCommand.parse(msg)
            #if cmd.file == None:
            #    raise DecodeError()
        except (DecodeError, IndexError) as exc:
            logging.warning('Parameter interest decoding failed')
            return
        aio.ensure_future(self._process_insert(cmd))

    async def _process_insert(self, cmd: InsertCommand):
        """
        Process insert command.
        """
        # print("Process Insert Command for File: ")
        # print("receive INSERT command for file: {}".format(Name.to_str(cmd.file.file_name)))
        file_name = Name.to_str(cmd.file.file_name)
        packets = cmd.file.packets
        packet_size = cmd.file.packet_size
        size = cmd.file.size
        fetch_path = cmd.fetch_path
        expiration_time = int(time.time() + (self.config['file_expiration'] * 60)) # convert hours to seconds
        # Set file to not expire if file_expiration in config is set to 0
        if self.config['file_expiration'] == 0:
            expiration_time = 0
        self.logger.info(f"[CMD][INSERT]   file {file_name}")

        nodes = self.global_view.get_nodes()
        desired_copies = self.replication_degree
        if len(nodes) < (desired_copies * 2):
            self.logger.warning("not enough nodes") # TODO: notify the client?
            return

        # select sessions
        random.shuffle(nodes)
        picked_nodes = random.sample(nodes, (desired_copies * 2))
        pickself = False
        for i in range(desired_copies):
            if picked_nodes[i]['node_name'] == self.config['node_name']:
                pickself = True
                break

        # if pickself:
        #     # TODO: fetch and store this file
        #     pass
            # print("pick myself")
            # picked_sessions = list(filter(lambda x: x['id'] != self.config['session_id'], picked_sessions))

        backups = []
        backup_list = []
        for picked_node in picked_nodes:
            node_name = picked_node['node_name']
            nonce = secrets.token_hex(4)
            backup = BackupTlv()
            backup.node_name = node_name.encode()
            backup.nonce = nonce.encode()
            backups.append(backup)
            backup_list.append((node_name, nonce))

        # add tlv
        favor = 1.85
        add_message = AddMessageTlv()
        add_message.node_name = self.config['node_name'].encode()
        add_message.favor = str(favor).encode()
        add_message.file = File()
        add_message.file.file_name = cmd.file.file_name
        add_message.file.packets = packets
        add_message.file.packet_size = packet_size
        add_message.file.size = size
        add_message.desired_copies = desired_copies
        add_message.fetch_path = FetchPathTlv()
        add_message.fetch_path.prefix = fetch_path
        add_message.is_stored_by_origin = 0
        add_message.expiration_time = expiration_time
        add_message.backup_list = backups
        # add msg
        message = Message()
        message.type = MessageTypes.ADD
        message.value = add_message.encode()
        # apply globalview and send msg thru SVS
        try:
            next_state_vector = self.main_loop.svs.getCore().getStateTable().getSeqno(Name.to_str(Name.from_str(self.config['node_name']))) + 1
        except TypeError:
            next_state_vector = 0
        self.global_view.add_file(
            file_name,
            size,
            self.config['node_name'],
            Name.to_str(fetch_path),
            packet_size,
            packets=packets,
            desired_copies=desired_copies,
            expiration_time=expiration_time,
        )
        if pickself:
            # self.global_view.store_file(insertion_id, self.config['session_id'])
            self.main_loop.fetch_file(file_name, packets, packet_size, Name.to_str(fetch_path))
        self.global_view.set_backups(file_name, backup_list)
        self.main_loop.svs.publishData(message.encode())
        bak = ""
        for backup in backup_list:
            bak = bak + backup[0] + ","
        self.logger.info(f"[MSG][ADD]*     nam={self.config['node_name']};fil={file_name};cop={desired_copies};pck={packets};pck_size={packet_size};siz={size};bak={bak};exp={expiration_time}")