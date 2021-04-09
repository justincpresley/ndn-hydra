import asyncio as aio
import logging
import random
import secrets
import sys
import time
from ndn.app import NDNApp
from ndn.encoding import Name, NonStrictName, Component, DecodeError
from ndn_python_repo import Storage
from . import ReadHandle, ProtocolHandle
from ..protocol.repo_commands import RepoCommand
from ..utils import PubSub
from ..global_view_2 import GlobalView
from ..repo_messages.add import FileTlv, FetchPathTlv, BackupTlv, AddMessageBodyTlv
from ..repo_messages.message import MessageTlv, MessageTypes
from ..handle_messages import MessageHandle

class InsertCommandHandle(ProtocolHandle):
    """
    InsertCommandHandle processes insert command handles, and deletes corresponding data stored
    in the database.
    TODO: Add validator
    """
    def __init__(self, app: NDNApp, storage: Storage, pb: PubSub, read_handle: ReadHandle,
                 config: dict, message_handle: MessageHandle, global_view: GlobalView):
        """
        Read handle need to keep a reference to write handle to register new prefixes.
        :param app: NDNApp.
        :param storage: Storage.
        :param read_handle: ReadHandle. This param is necessary because InsertCommandHandle need to
            unregister prefixes.
        """
        super(InsertCommandHandle, self).__init__(app, storage, pb, config)
        self.m_read_handle = read_handle
        self.prefix = None
        self.message_handle = message_handle
        self.global_view = global_view
        # print(type(self.config['session_id']))
        #self.register_root = config['repo_config']['register_root']

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
        Return to client with status code 100 immediately, and then start data fetching process.
        """
        # print("Process Insert Command for File: ")
        # print("receive INSERT command for file: {}".format(Name.to_str(cmd.file.file_name)))
     
        file_name = cmd.file.file_name
        desired_copies = cmd.file.desired_copies
        packets = cmd.file.packets
        size = cmd.file.size
        sequence_number = cmd.sequence_number
        fetch_path = cmd.fetch_path.prefix

        print("[CMD] receive INSERT CMD for file {}".format(file_name))

        # TODO: check duplicate sequence number

        # is there are enough sessions
        if desired_copies == 0:
            print("desired_copies is 0")
            return

        sessions = self.global_view.get_sessions()

        if len(sessions) < (desired_copies * 2):
            print("not enough node sessions") # TODO: notify the client?
            return

        # generate unique insertion_id
        insertion_id = secrets.token_hex(8)
        while self.global_view.get_insertion(insertion_id) != None:
            insertion_id = secrets.token_hex(8)

        # select sessions
        needed = desired_copies * 2
        #   itself?
        rdm = random.random()
        pickself = (rdm >= 0.50)
        
        
        if pickself:
            # TODO: fetch and store this file
            print("pick myself")
            needed -= 1
            sessions = list(filter(lambda x: x['id'] != self.config['session_id'], sessions))


        picked_sessions = random.sample(sessions, needed)
        
        random.shuffle(picked_sessions)
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
        expire_at = int(time.time()+600)
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
        add_message_body.file.size = size
        add_message_body.sequence_number = sequence_number
        add_message_body.fetch_path = FetchPathTlv()
        add_message_body.fetch_path.prefix = fetch_path
        add_message_body.is_stored_by_origin = 1 if pickself else 0
        add_message_body.backup_list = backups
        # add msg
        add_message = MessageTlv()
        add_message.header = MessageTypes.ADD
        add_message.body = add_message_body.encode()
        # apply globalview and send msg thru SVS
        next_state_vector = self.message_handle.svs.getCore().getStateVector().get(self.config['session_id']) + 1
        self.global_view.add_insertion(
            insertion_id, 
            Name.to_str(file_name), 
            sequence_number, 
            size, 
            self.config['session_id'],
            Name.to_str(fetch_path),
            next_state_vector,
            packets,
            desired_copies
        )
        if pickself:
            self.global_view.store_file(insertion_id, self.config['session_id'])
        self.global_view.set_backups(insertion_id, backup_list)
        self.message_handle.svs.publishData(add_message.encode())
        
        

    
