import asyncio as aio
from ndn.app import NDNApp
from ndn.encoding import Name
from ..svs import SVS_Socket
from ..repo_messages import *
from ..storage import *

class MessageHandle:
    def __init__(self, app:NDNApp, storage:Storage, group_prefix:str, node_id:str, cache_others:bool):
        self.app = app
        self.storage = storage
        self.group_prefix = Name.from_str(group_prefix)
        self.node_id = Name.from_str(node_id)
        self.cache_others = cache_others
        self.svs_socket = None
        self.global_view = None

    async def start(self):
        self.svs_socket = SVS_Socket(self.app, self.storage, self.group_prefix, self.node_id, self.missing_callback, self.cache_others)

    def missing_callback(self, missing_list):
        aio.ensure_future(self.on_missing_messages(missing_list))

    async def on_missing_messages(self, missing_list):
        for i in missing_list:
            while i.lowSeqNum <= i.highSeqNum:
                # print('{}:{}, {}'.format(i.nid, i.lowSeqNum, i.highSeqNum))
                message_bytes = await self.svs_socket.fetchData(i.nid, i.lowSeqNum)
                if message_bytes == None:
                    continue
                nid = i.nid
                seq = i.lowSeqNum
                message = Message(nid, seq, message_bytes)
                message_body = message.get_message_body()
                aio.ensure_future(message_body.apply(self.global_view))
                print('fetched GM {}:{}'.format(nid, seq))
                i.lowSeqNum = i.lowSeqNum + 1
