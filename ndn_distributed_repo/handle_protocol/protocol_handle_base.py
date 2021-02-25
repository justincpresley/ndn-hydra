import asyncio as aio
import logging
from ndn.app import NDNApp
from ndn.encoding import Name, Component, NonStrictName, FormalName
from ndn.encoding.tlv_model import DecodeError
from typing import List

from ..protocol.repo_commands import RepoCommand
from ..storage import Storage
from ..utils import PubSub


class ProtocolHandle(object):
    """
    Interface for protocol interest handles
    """
    def __init__(self, app: NDNApp, storage: Storage, pb: PubSub, config: dict):
        self.app = app
        self.storage = storage
        self.pb = pb
        self.config = config

    async def listen(self, prefix: Name):
        raise NotImplementedError
