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
from typing import Optional
from ndn.app import NDNApp
from ndn.encoding import Name, FormalName, InterestParam, BinaryStr, SignaturePtrs, Component
from ndn.storage import Storage
from ndn_hydra.repo.protocol.base_models import File
from ndn_hydra.repo.protocol.command_models import Command, InsertCommand, DeleteCommand, Status, FirstContact, CommandTypes
from ndn_hydra.repo.protocol.status_code import StatusCode
from ndn_hydra.repo.group_messages.remove import RemoveMessageTlv
from ndn_hydra.repo.group_messages.message import Message, MessageTypes
from ndn_hydra.repo.main.main_loop import MainLoop
from ndn_hydra.repo.handles.handle import Handle
from ndn_hydra.repo.modules.command_table import CommandTable
from ndn_hydra.repo.modules.global_view import GlobalView

class CommandHandle(Handle):
    def __init__(self, app:NDNApp, config:dict, global_view:GlobalView, data_storage:Storage, command_table:CommandTable, main_loop:MainLoop) -> None:
        self.app = app
        self.data_storage = data_storage
        self.main_loop = main_loop
        self.global_view = global_view
        self.config = config
        self.node_name = Name.from_str(config['node_name'])
        self.repo_prefix = Name.from_str(config['repo_prefix'])
        self.command_table = command_table
        self.replication_degree = config['replication_degree']
        self.request_prefix = self.repo_prefix + [Component.from_str("request")]
        self.status_prefix = self.repo_prefix + [Component.from_str("status")]
        self.node_request_prefix = self.node_name + self.repo_prefix + [Component.from_str("request")]
        self.node_status_prefix = self.node_name + self.repo_prefix + [Component.from_str("status")]
    async def listen(self) -> None:
        await self.app.register(self.status_prefix, self.onStatusInterest, need_sig_ptrs=True)
        logging.info(f'ComandHandle: listening {Name.to_str(self.status_prefix)}')
        await self.app.register(self.node_status_prefix, self.onStatusInterest, need_sig_ptrs=True)
        logging.info(f'ComandHandle: listening {Name.to_str(self.node_status_prefix)}')
        await self.app.register(self.request_prefix, self.onRequestInterest, need_sig_ptrs=True)
        logging.info(f'ComandHandle: listening {Name.to_str(self.request_prefix)}')
        await self.app.register(self.node_request_prefix, self.onRequestInterest, need_sig_ptrs=True)
        logging.info(f'ComandHandle: listening {Name.to_str(self.node_request_prefix)}')
    def onRequestInterest(self, int_name:FormalName, int_param:InterestParam, app_param:Optional[BinaryStr], sig_ptrs:SignaturePtrs) -> None:
        # if unauthenticated
        # if unauthorized to do commands
        # if cant read
        try:
            contact = FirstContact.parse(bytes(app_param))
            curi = Name.to_str(contact.curi)
        except (DecodeError, IndexError) as e:
            self._send_status_packet(int_name, None, StatusCode.BAD_REQUEST)
            return
        # if incorrect curi structure
        # if duplicate
        if self.command_table.get(curi) == None:
            self._send_status_packet(int_name, None, StatusCode.DUPLICATE)
            return
        # if server is having problems
        # accepted request
        self.command_table.set(curi, StatusCode.OK, None)
        self._send_status_packet(int_name, curi, StatusCode.OK)
        logging.info(f"[USR][REQUEST]   curi {curi}")
        # get command
        aio.get_event_loop().create_task(self._handle_client(curi, contact.prefix))
    def onStatusInterest(self, int_name:FormalName, int_param:InterestParam, app_param:Optional[BinaryStr], sig_ptrs:SignaturePtrs) -> None:
        # if unauthenticated
        # if authorize to view status
        # serve status
        curi = bytes(app_param).decode()
        stat = self.command_table.get(curi).code
        self._send_status_packet(int_name, curi, stat)
    def _send_status_packet(self, int_name:Name, curi:Optional[str], code:StatusCode) -> None:
        stat = Status()
        stat.curi = Name.from_str(curi)
        stat.code = code.value
        stat.node = self.node_name
        self.app.put_raw_packet(self.app.prepare_data(int_name, stat.encode(), meta_info=MetaInfo(freshness_period=500, final_block_id=None)))
    async def _handle_client(self, curi:str, client_prefix:Name) -> None:
        command = await self._retrieve_command(curi, client_prefix)
        if command == None:
            return
        self.command_table.set(curi, StatusCode.STAND_BY, command)
        if command.type == CommandTypes.INSERT:
            logging.info(f"[CMD][INSERT]   file {file_name}")
            self._process_insert(curi, command.specify())
        elif command.type == CommandTypes.DELETE:
            logging.info(f"[CMD][DELETE]   file {file_name}")
            self._process_delete(curi, command.specify())
        else:
            self.command_table.set(curi, StatusCode.BAD_COMMAND, None)
    async def _retrieve_command(self, curi:str, client_prefix:Name) -> Optional[Command]:
        name = client_prefix + self.repo_prefix + [Component.from_str("command")]
        try:
            int_name, meta_info, content = await self.app.express_interest(name, must_be_fresh=True, can_be_prefix=True, lifetime=6000)
            # if authenticated and authorized
            try:
                command = Command.parse(bytes(content))
                self.command_table.set(curi, StatusCode.OK, command)
                return command
            except (DecodeError, IndexError) as e:
                self.command_table.set(curi, StatusCode.BAD_COMMAND, None)
                return None
        except Exception as e:
            self.command_table.set(curi, StatusCode.NOT_FOUND, None)
            return None

    def _process_delete(self, curi:str, cmd:DeleteCommand):
        file_name = Name.to_str(cmd.file_name)
        if self.global_view.get_file(file_name) == None:
            self.command_table.set(curi, StatusCode.GHOST_FILE, Command.from_delete(cmd))
            return
        favor = 1.85 # change to FavorCalculator
        remove_message = RemoveMessageTlv()
        remove_message.node_name = Name.to_str(self.node_name).encode()
        remove_message.favor = str(favor).encode()
        remove_message.file_name = cmd.file_name
        message = Message()
        message.type = MessageTypes.REMOVE
        message.value = remove_message.encode()
        self.global_view.delete_file(file_name)
        self.main_loop.svs.publishData(message.encode())
        self.command_table.set(curi, StatusCode.FULFILLED, Command.from_delete(cmd))
        logging.info(f"[MSG][REMOVE]*  fil={file_name}")

    def _process_insert(self, curi:str, cmd:InsertCommand):
        # work on insert cmd
        file_name = Name.to_str(cmd.file.name)
        packets = cmd.file.packets
        packet_size = cmd.file.packet_size
        size = cmd.file.size
        fetch_path = cmd.fetch_path

        nodes = self.global_view.get_nodes()
        desired_copies = self.replication_degree
        if len(nodes) < (desired_copies * 2):
            logging.warning("not enough nodes") # TODO: notify the client?
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
        add_message.file.file_name = cmd.file.name
        add_message.file.packets = packets
        add_message.file.packet_size = packet_size
        add_message.file.size = size
        add_message.desired_copies = desired_copies
        add_message.fetch_path = FetchPathTlv()
        add_message.fetch_path.prefix = fetch_path
        add_message.is_stored_by_origin = 0
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
            desired_copies=desired_copies
        )
        if pickself:
            # self.global_view.store_file(insertion_id, self.config['session_id'])
            self.main_loop.fetch_file(file_name, packets, packet_size, Name.to_str(fetch_path))
        self.global_view.set_backups(file_name, backup_list)
        self.main_loop.svs.publishData(message.encode())
        bak = ""
        for backup in backup_list:
            bak = bak + backup[0] + ","

        self.command_table.set(Name.to_str(contact.curi), StatusCode.FULFILLED, Command.from_insert(cmd))
        logging.info(f"[MSG][ADD]*     nam={self.config['node_name']};fil={file_name};cop={desired_copies};pck={packets};pck_size={packet_size};siz={size};bak={bak}")