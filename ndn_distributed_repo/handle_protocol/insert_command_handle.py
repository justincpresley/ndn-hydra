import asyncio as aio
import logging
import random
import sys
import time
from ndn.app import NDNApp
from ndn.encoding import Name, NonStrictName, Component, DecodeError
from . import ReadHandle, ProtocolHandle
from ..protocol.repo_commands import RepoCommand
from ..storage import Storage
from ..utils import PubSub
from ..svs import SVS_Socket
from ..repo_messages.add import FileTlv, FileOriginalPathTlv, AddMessageBodyTlv
from ..repo_messages.message import MessageTlv, MessageTypes
from ..handle_messages import MessageHandle

class InsertCommandHandle(ProtocolHandle):
    """
    InsertCommandHandle processes insert command handles, and deletes corresponding data stored
    in the database.
    TODO: Add validator
    """
    def __init__(self, app: NDNApp, storage: Storage, pb: PubSub, read_handle: ReadHandle,
                 config: dict, message_handle: MessageHandle):
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
        print("receive INSERT command for file: {}".format(Name.to_str(cmd.file.name)))
        add_message_body = AddMessageBodyTlv()
        add_message_body.insertion_id = 1
        add_message_body.node_id = self.config['node_id'].encode('utf-8')
        add_message_body.favor = 20
        add_message_body.valid_thru = int(time.time()) + 30
        add_message_body.file = FileTlv()
        add_message_body.file.name = Name.from_str('/foo/bar.txt')
        add_message_body.file.copies = 3
        add_message_body.file.size = 1024
        add_message_body.file.blocks = 2
        add_message_body.file_original_path = FileOriginalPathTlv()
        add_message_body.file_original_path.name = Name.from_str('/client/upload/foo/bar.txt')
        add_message_body.file_seq = 1 
        add_message = MessageTlv()
        add_message.header = MessageTypes.ADD
        add_message.body = add_message_body.encode()
        raw = add_message.encode()
        # print(raw)
        print('produce GM {}'.format(self.config['node_id']))
        self.message_handle.svs_socket.publishData(raw)
