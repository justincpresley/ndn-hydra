import asyncio as aio
import logging
import random
import sys
from ndn.app import NDNApp
from ndn.encoding import Name, NonStrictName, Component, DecodeError
from . import ReadHandle, CommandHandle
from ..protocol.repo_commands import RepoCommand
from ..storage import Storage
from ..utils import PubSub


class InsertCommandHandle(ProtocolHandle):
    """
    InsertCommandHandle processes insert command handles, and deletes corresponding data stored
    in the database.
    TODO: Add validator
    """
    def __init__(self, app: NDNApp, storage: Storage, pb: PubSub, read_handle: ReadHandle,
                 config: dict):
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
        self.register_root = config['repo_config']['register_root']

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
        await self._schedule_announce_process_status(period=3)

    def _on_insert_msg(self, msg):
        try:
            cmd = RepoCommand.parse(msg)
            if cmd.name == None:
                raise DecodeError()
        except (DecodeError, IndexError) as exc:
            logging.warning('Parameter interest decoding failed')
            return
        aio.ensure_future(self._process_insert(cmd))

    async def _process_insert(self, cmd: RepoCommand):
        """
        Process insert command.
        Return to client with status code 100 immediately, and then start data fetching process.
        """ 
