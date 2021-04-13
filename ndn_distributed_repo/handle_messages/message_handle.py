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

    def heartbeat(self):
        # hb tlv
        expire_at = int(time.time()+(self.config['period']*2))
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
        # print("state_vector: {0}".format(self.svs.getCore().getStateVector().to_str()))
        next_state_vector = self.svs.getCore().getStateVector().get(Name.to_str(Name.from_str(self.svs_node_id))) + 1
        self.global_view.update_session(self.svs_node_id, self.repo_node_name, expire_at, favor, next_state_vector)
        self.svs.publishData(heartbeat_message.encode())

    def detect_expired_sessions(self):
        deadline = int(time.time()) - (self.config['period'])
        expired_sessions = self.global_view.get_sessions_expired_by(deadline)
        for expired_session in expired_sessions:
            # generate expire msg and send
            # expire tlv
            expire_at = int(time.time()+(self.config['period']*2))
            favor = 1.85
            expire_message_body = ExpireMessageBodyTlv()
            expire_message_body.session_id = self.config['session_id'].encode()
            expire_message_body.node_name = self.config['node_name'].encode()
            expire_message_body.expire_at = expire_at
            expire_message_body.favor = str(favor).encode()
            expire_message_body.expired_session_id = expired_session['id'].encode()
            # expire msg
            expire_message = MessageTlv()
            expire_message.header = MessageTypes.EXPIRE
            expire_message.body = expire_message_body.encode()
            # apply globalview and send msg thru SVS
            self.global_view.expire_session(expired_session['id'])
            self.svs.publishData(expire_message.encode())
            val = "[MSG][EXPIRE]* sid={sid};exp_sid={esid}".format(
                sid=self.config['session_id'],
                esid=expired_session['id']
            )
            print(val)
        
        # am I at the top of any insertion's backup list?
        underreplicated_insertions = self.global_view.get_underreplicated_insertions()
        for underreplicated_insertion in underreplicated_insertions:
            deficit = underreplicated_insertion['desired_copies'] - len(underreplicated_insertion['stored_bys'])
            for backuped_by in underreplicated_insertion['backuped_bys']:
                if (backuped_by['session_id'] == self.config['session_id']) and (backuped_by['rank'] < deficit):
                    # generate store msg and send
                    # store tlv
                    expire_at = int(time.time()+(self.config['period']*2))
                    favor = 1.85
                    store_message_body = StoreMessageBodyTlv()
                    store_message_body.session_id = self.config['session_id'].encode()
                    store_message_body.node_name = self.config['node_name'].encode()
                    store_message_body.expire_at = expire_at
                    store_message_body.favor = str(favor).encode()
                    store_message_body.insertion_id = underreplicated_insertion['id'].encode()
                    # store msg
                    store_message = MessageTlv()
                    store_message.header = MessageTypes.STORE
                    store_message.body = store_message_body.encode()
                    # apply globalview and send msg thru SVS
                    # next_state_vector = svs.getCore().getStateVector().get(config['session_id']) + 1
                    self.global_view.store_file(underreplicated_insertion['id'], self.config['session_id'])
                    self.svs.publishData(store_message.encode())
                    val = "[MSG][STORE]+  sid={sid};iid={iid}".format(
                        sid=self.config['session_id'],
                        iid=underreplicated_insertion['id']
                    )
                    print(val)


    def periodic(self):
        # print('periodic')
        # periodic tasks:
        # 1. I am Alive
        # 2. check expired nodes
        # 3. make possible claims
        self.heartbeat()
        self.detect_expired_sessions()

        sessions = self.global_view.get_sessions()
        insertions = self.global_view.get_insertions()

        for insertion in insertions:
            on = ""
            for stored_by in insertion['stored_bys']:
                on = on + stored_by + ","
            bck = ""
            for backuped_by in insertion['backuped_bys']:
                bck = bck + backuped_by['session_id'] + ","
            val = '[GV]           iid={iid}; name={name}; on={on}; bck={bck}'.format(
                iid=insertion['id'],
                name=insertion['file_name'],
                on=on,
                bck=bck
            )
            print(val)
        # print("--")
     
    # the main coroutine
    async def start(self):
        self.svs = SVSync(self.app, Name.from_str(self.svs_group_prefix), Name.from_str(self.svs_node_id), self.svs_missing_callback, storage=self.svs_storage)
        #publish first heartbeat


        while True:
            await aio.sleep(self.config['period'])
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
