# ----------------------------------------------------------
# NDN Hydra MainLoop
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
import secrets
import time
import random
from typing import Dict, List
from ndn.app import NDNApp
from ndn.encoding import Name, Component
from ndn.types import InterestNack, InterestTimeout
from ndn.svs import SVSync
from ndn_python_repo import Storage, SqliteStorage
from ndn_hydra.repo.global_view.global_view import GlobalView
from ndn_hydra.repo.repo_messages import *
from ndn_hydra.repo.utils.concurrent_fetcher import concurrent_fetcher

class MainLoop:
    def __init__(self, app: NDNApp, config: Dict, global_view: GlobalView, data_storage: Storage):
        self.app = app
        self.config = config
        self.global_view = global_view
        self.data_storage = data_storage
        self.svs_storage = SqliteStorage(self.config['svs_storage_path'])
        self.svs = None
        self.expire_at = 0
        self.logger = logging.getLogger()

    # the main coroutine
    async def start(self):
        self.svs = SVSync(self.app, Name.normalize(self.config['repo_prefix'] + "/group"), Name.normalize(self.config['session_id']), self.svs_missing_callback, storage=self.svs_storage)
        while True:
            await aio.sleep(self.config['period'])
            self.periodic()

    # def __init__(self, app:NDNApp, svs_storage:Storage, session_id:str, node_name:str, svs_cache_others:bool, global_view: GlobalView, config):
    #     self.app = app
    #     self.svs_storage = svs_storage
    #     self.svs_group_prefix = repo_prefix + "/group"
    #     self.svs_node_id = session_id
    #     self.repo_node_name = node_name
    #     self.svs_cache_others = svs_cache_others
    #     self.svs = None
    #     self.global_view = global_view
    #     self.config = config
    #     self.expire_at = 0

    def heartbeat(self):
        # TODO: skip if expire_at value is big enough
        # hb tlv
        expire_at = int(time.time()+(self.config['period']*2))
        self.expire_at = expire_at
        favor = 1.85
        heartbeat_message_body = HeartbeatMessageBodyTlv()
        heartbeat_message_body.session_id = self.config['session_id'].encode()
        heartbeat_message_body.node_name = self.config['node_name'].encode()
        heartbeat_message_body.expire_at = expire_at
        heartbeat_message_body.favor = str(favor).encode()

        # hb msg
        heartbeat_message = MessageTlv()
        heartbeat_message.header = MessageTypes.HEARTBEAT
        heartbeat_message.body = heartbeat_message_body.encode()

        # heartbeat_message_body = HeartbeatMessageBody(self.svs_node_id, self.state_vector, heartbeat_message_body.encode())
        # print("state_vector: {0}".format(self.svs.getCore().getStateVector().to_str()))
        next_state_vector = self.svs.getCore().getStateVector().get(Name.to_str(Name.from_str(self.config['session_id']))) + 1
        self.global_view.update_session(self.config['session_id'], self.config['node_name'], expire_at, favor, next_state_vector)
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
            self.logger.info(val)

        # am I at the top of any insertion's backup list?
        underreplicated_insertions = self.global_view.get_underreplicated_insertions()
        for underreplicated_insertion in underreplicated_insertions:
            deficit = underreplicated_insertion['desired_copies'] - len(underreplicated_insertion['stored_bys'])
            for backuped_by in underreplicated_insertion['backuped_bys']:
                if (backuped_by['session_id'] == self.config['session_id']) and (backuped_by['rank'] < deficit):
                    self.fetch_file(underreplicated_insertion['id'], underreplicated_insertion['file_name'], underreplicated_insertion['packets'], underreplicated_insertion['digests'], underreplicated_insertion['fetch_path'])


    def claim(self):
        # TODO: possibility based on # active sessions and period
        # if random.random() < 0.618:
        #     return
        backupable_insertions = self.global_view.get_backupable_insertions()

        for backupable_insertion in backupable_insertions:

            if random.random() < 0.618:
                continue
            # print(json.dumps(backupable_insertion['stored_bys']))
            # print(json.dumps(backupable_insertion['backuped_bys']))
            already_in = False
            for stored_by in backupable_insertion['stored_bys']:
                if stored_by == self.config['session_id']:
                    already_in = True
                    break
            for backuped_by in backupable_insertion['backuped_bys']:
                if backuped_by['session_id'] == self.config['session_id']:
                    already_in = True
                    break
            if already_in == True:
                continue
            if len(backupable_insertion['backuped_bys']) == 0 and len(backupable_insertion['stored_bys']) == 0:
                continue
            authorizer = None
            if len(backupable_insertion['backuped_bys']) == 0:
                authorizer = {
                    'session_id': backupable_insertion['stored_bys'][-1],
                    'rank': -1,
                    'nonce': backupable_insertion['id']
                }
            else:
                authorizer = backupable_insertion['backuped_bys'][-1]
            # generate claim (request) msg and send
            # claim tlv
            expire_at = int(time.time()+(self.config['period']*2))
            favor = 1.85
            claim_message_body = ClaimMessageBodyTlv()
            claim_message_body.session_id = self.config['session_id'].encode()
            claim_message_body.node_name = self.config['node_name'].encode()
            claim_message_body.expire_at = expire_at
            claim_message_body.favor = str(favor).encode()
            claim_message_body.insertion_id = backupable_insertion['id'].encode()
            claim_message_body.type = ClaimMessageTypes.REQUEST
            claim_message_body.claimer_session_id = self.config['session_id'].encode()
            claim_message_body.claimer_nonce = secrets.token_hex(4).encode()
            claim_message_body.authorizer_session_id = authorizer['session_id'].encode()
            claim_message_body.authorizer_nonce = authorizer['nonce'].encode()
            # claim msg
            claim_message = MessageTlv()
            claim_message.header = MessageTypes.CLAIM
            claim_message.body = claim_message_body.encode()
            self.svs.publishData(claim_message.encode())
            val = "[MSG][CLAIM.R]*sid={sid};iid={iid}".format(
                sid=self.config['session_id'],
                iid=backupable_insertion['id']
            )
            self.logger.info(val)


    def periodic(self):
        # print('periodic')
        # periodic tasks:
        self.heartbeat()
        self.detect_expired_sessions()
        self.claim()
        # self.store()

        # sessions = self.global_view.get_sessions()
        # insertions = self.global_view.get_insertions()

        # for insertion in insertions:
        #     on = ""
        #     for stored_by in insertion['stored_bys']:
        #         on = on + stored_by + ","
        #     bck = ""
        #     for backuped_by in insertion['backuped_bys']:
        #         bck = bck + backuped_by['session_id'] + ","
        #     val = '[GV]           iid={iid}; name={name}; on={on}; bck={bck}'.format(
        #         iid=insertion['id'],
        #         name=insertion['file_name'],
        #         on=on,
        #         bck=bck
        #     )
            # self.logger.info(val)
        # print("--")

    def store(self, insertion_id: str):
        insertion = self.global_view.get_insertion(insertion_id)
        if len(insertion['stored_bys']) < insertion['desired_copies']:
            # store msg
            expire_at = int(time.time()+(self.config['period']*2))
            favor = 1.85
            store_message_body = StoreMessageBodyTlv()
            store_message_body.session_id = self.config['session_id'].encode()
            store_message_body.node_name = self.config['node_name'].encode()
            store_message_body.expire_at = expire_at
            store_message_body.favor = str(favor).encode()
            store_message_body.insertion_id = insertion_id.encode()
            # store msg
            store_message = MessageTlv()
            store_message.header = MessageTypes.STORE
            store_message.body = store_message_body.encode()
            # apply globalview and send msg thru SVS
            # next_state_vector = svs.getCore().getStateVector().get(config['session_id']) + 1

            self.global_view.store_file(insertion_id, self.config['session_id'])
            self.svs.publishData(store_message.encode())
            val = "[MSG][STORE]*  sid={sid};iid={iid}".format(
                sid=self.config['session_id'],
                iid=insertion_id
            )
            self.logger.info(val)

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
                aio.ensure_future(message_body.apply(self.global_view, self.fetch_file, self.svs, self.config))
                # print('fetched GM {}:{}'.format(nid, seq))
                i.lowSeqNum = i.lowSeqNum + 1

    def svs_sending_callback(self, expire_at: int):
        self.expire_at = expire_at

    def fetch_file(self, insertion_id: str, file_name: str, packets: int, digests: List[bytes], fetch_path: str):
        val = "[ACT][FETCH]*  iid={iid};file_name={file_name};pcks={packets};fetch_path={fetch_path}".format(
            iid=insertion_id,
            file_name=file_name,
            packets=packets,
            fetch_path=fetch_path
        )
        self.logger.info(val)
        aio.ensure_future(self.async_fetch(insertion_id, file_name, packets, digests, fetch_path))

    async def async_fetch(self, insertion_id: str, file_name: str, packets: int, digests: List[bytes], fetch_path: str):
        self.logger.debug(packets)
        if packets > 1:
            start = time.time()
            inserted_packets = await self.fetch_segmented_file(file_name, packets, fetch_path)
            if inserted_packets == packets:
                end = time.time()
                duration = end -start
                val = "[ACT][FETCHED]*pcks={packets};duration={duration}".format(
                    packets=packets,
                    duration=duration
                )
                self.logger.info(val)
                self.store(insertion_id)
        elif packets == 1:
            inserted_packets = await self.fetch_single_file(file_name, fetch_path)
            if inserted_packets == packets:
                self.store(insertion_id)

    async def fetch_segmented_file(self, file_name: str, packets: int, fetch_path: str):
        semaphore = aio.Semaphore(10)
        fetched_segments = 0
        async for (_, _, content, data_bytes, key) in concurrent_fetcher(self.app, fetch_path, file_name, 0, packets-1, semaphore):
            #TODO: check digest
            # print("segment:")
            # print(Name.to_str(key))
            # print(type(content))
            # print(content)
            # print(type(data_bytes))
            # print(data_bytes)
            self.data_storage.put_data_packet(key, data_bytes)
            # self.data_storage.put_data_packet(key, content.tobytes())
            fetched_segments += 1
        return fetched_segments

    async def fetch_single_file(self, file_name: str, fetch_path: str):
        int_name = int_name = Name.normalize(fetch_path) + [Component.from_segment(0)]
        key = Name.normalize(file_name) + [Component.from_segment(0)]
        try:
            data_name, _, _, data_bytes = await self.app.express_interest(
                int_name, need_raw_packet=True, can_be_prefix=False, lifetime=1000)
        except InterestNack as e:
            return 0
        except InterestTimeout:
            return 0
        self.data_storage.put_data_packet(key, data_bytes)
        return 1