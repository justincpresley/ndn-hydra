import asyncio as aio
import time
from ndn.app import NDNApp
from ndn.encoding import Name
from ndn.svs import SVSync
from ndn_python_repo import Storage
from ..repo_messages import *

class MessageHandle:
    def __init__(self, app:NDNApp, svs_storage:Storage, svs_group_prefix:str, session_id:str, node_name:str, svs_cache_others:bool, global_view, config):
        self.app = app
        self.svs_storage = svs_storage
        self.svs_group_prefix = svs_group_prefix
        self.svs_node_id = session_id
        self.repo_node_name = node_name
        self.svs_cache_others = svs_cache_others
        self.svs = None
        self.global_view = global_view
        self.config = config

    def periodic(self):
        # hb tlv
        expire_at = int(time.time()+600)
        favor = 1.85
        heartbeat_message_body = HeartbeatMessageBodyTlv()
        heartbeat_message_body.session_id = self.svs_node_id.encode()
        heartbeat_message_body.node_name = self.repo_node_name.encode()
        heartbeat_message_body.expire_at = expire_at
        heartbeat_message_body.favor = str(favor).encode()

        # hb msg
        heartbeat_message = MessageTlv()
        heartbeat_message.header = MessageTypes.HEARTBEAT
        heartbeat_message.body = heartbeat_message_body.encode()

        # heartbeat_message_body = HeartbeatMessageBody(self.svs_node_id, self.state_vector, heartbeat_message_body.encode())
        next_state_vector = self.svs.getCore().getStateVector().get(self.svs_node_id) + 1
        self.global_view.update_session(self.svs_node_id, self.repo_node_name, expire_at, favor, next_state_vector)
        self.svs.publishData(heartbeat_message.encode())

        sessions = self.global_view.get_sessions()
        # print(json.dumps(sessions))

        insertions = self.global_view.get_insertions()
        for insertion in insertions:
            on = ""
            for stored_by in insertion['stored_bys']:
                on = on + stored_by + ","
            bck = ""
            for backuped_by in insertion['backuped_bys']:
                bck = bck + backuped_by['session_id'] + ","
            val = 'iid={iid}; name={name}; on={on}; bck={bck}'.format(
                iid=insertion['id'],
                name=insertion['file_name'],
                on=on,
                bck=bck
            )
            print(val)
        print("\n")

        # print('periodic')
        # periodic tasks:
        # 1. I am Alive
        # 2. check expired nodes
        # 3. make possible claims
            
    # the main coroutine
    async def start(self):
        self.svs = SVSync(self.app, Name.from_str(self.svs_group_prefix), Name.from_str(self.svs_node_id), self.svs_missing_callback, storage=self.svs_storage)
        #publish first heartbeat


        while True:
            await aio.sleep(5)
            self.periodic()
            

    def svs_missing_callback(self, missing_list):
        aio.ensure_future(self.on_missing_svs_messages(missing_list))

    async def on_missing_svs_messages(self, missing_list):
        for i in missing_list:
            while i.lowSeqNum <= i.highSeqNum:
                # print('{}:{}, {}'.format(i.nid, i.lowSeqNum, i.highSeqNum))
                message_bytes = await self.svs.fetchData(Name.from_str(i.nid), i.lowSeqNum)
                if message_bytes == None:
                    continue
                nid = i.nid
                seq = i.lowSeqNum
                message = Message(nid, seq, message_bytes)
                message_body = message.get_message_body()
                aio.ensure_future(message_body.apply(self.global_view, self.svs, self.config))
                # print('fetched GM {}:{}'.format(nid, seq))
                i.lowSeqNum = i.lowSeqNum + 1
