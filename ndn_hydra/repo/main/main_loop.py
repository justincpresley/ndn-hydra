# -------------------------------------------------------------
# NDN Hydra MainLoop
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
import secrets
import time
import random
from typing import Dict, List
from ndn.app import NDNApp
from ndn.encoding import Name, Component
from ndn.types import InterestNack, InterestTimeout
from ndn.svs import SVSync
from ndn.storage import Storage, SqliteStorage
from ndn_hydra.repo.modules.global_view import GlobalView
from ndn_hydra.repo.group_messages import *
from ndn_hydra.repo.utils.concurrent_fetcher import concurrent_fetcher

class MainLoop:
    def __init__(self, app: NDNApp, config: Dict, global_view: GlobalView, data_storage: Storage, svs_storage: Storage):
        self.app = app
        self.config = config
        self.global_view = global_view
        self.data_storage = data_storage
        self.svs_storage = svs_storage
        self.svs = None
        self.expire_at = 0
        self.logger = logging.getLogger()

    # the main coroutine
    async def start(self):
        self.svs = SVSync(self.app, Name.normalize(self.config['repo_prefix'] + "/group"), Name.normalize(self.config['node_name']), self.svs_missing_callback, storage=self.svs_storage)
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
        heartbeat_message = HeartbeatMessageTlv()
        heartbeat_message.node_name = self.config['node_name'].encode()
        heartbeat_message.expire_at = expire_at
        heartbeat_message.favor = str(favor).encode()
        # hb msg
        message = Message()
        message.type = MessageTypes.HEARTBEAT
        message.value = heartbeat_message.encode()

        # heartbeat_message = HeartbeatMessage(self.svs_node_id, self.state_vector, heartbeat_message.encode())
        # print("state_vector: {0}".format(self.svs.getCore().getStateVector().to_str()))
        try:
            next_state_vector = self.svs.getCore().getStateTable().getSeqno(Name.to_str(Name.from_str(self.config['node_name']))) + 1
        except TypeError:
            next_state_vector = 0
        self.global_view.update_node(self.config['node_name'], expire_at, favor, next_state_vector)
        self.svs.publishData(message.encode())

    def detect_expired_nodes(self):
        deadline = int(time.time()) - (self.config['period'])
        expired_nodes = self.global_view.get_nodes_expired_by(deadline)
        for expired_node in expired_nodes:
            # generate expire msg and send
            # expire tlv
            expire_at = int(time.time()+(self.config['period']*2))
            favor = 1.85
            expire_message = ExpireMessageTlv()
            expire_message.node_name = self.config['node_name'].encode()
            expire_message.expire_at = expire_at
            expire_message.favor = str(favor).encode()
            expire_message.expired_node_name = expired_node['node_name'].encode()
            # expire msg
            message = Message()
            message.type = MessageTypes.EXPIRE
            message.value = expire_message.encode()
            # apply globalview and send msg thru SVS
            self.global_view.expire_node(expired_node['node_name'])
            self.svs.publishData(message.encode())
            val = "[MSG][EXPIRE]* nam={nam};exp_nam={enam}".format(
                nam=self.config['node_name'],
                enam=expired_node['node_name']
            )
            self.logger.info(val)

        # am I at the top of any insertion's backup list?
        underreplicated_files = self.global_view.get_underreplicated_files()
        for underreplicated_file in underreplicated_files:
            deficit = underreplicated_file['desired_copies'] - len(underreplicated_file['stored_bys'])
            for backuped_by in underreplicated_file['backuped_bys']:
                if (backuped_by['node_name'] == self.config['node_name']) and (backuped_by['rank'] < deficit):
                    self.fetch_file(underreplicated_file['file_name'], underreplicated_file['packets'], underreplicated_file['digests'], underreplicated_file['fetch_path'])


    def claim(self):
        # TODO: possibility based on # active sessions and period
        # if random.random() < 0.618:
        #     return
        backupable_files = self.global_view.get_backupable_files()
        for backupable_file in backupable_files:
            if random.random() < 0.618:
                continue
            # print(json.dumps(backupable_insertion['stored_bys']))
            # print(json.dumps(backupable_insertion['backuped_bys']))
            already_in = False
            for stored_by in backupable_file['stored_bys']:
                if stored_by == self.config['node_name']:
                    already_in = True
                    break
            for backuped_by in backupable_file['backuped_bys']:
                if backuped_by['node_name'] == self.config['node_name']:
                    already_in = True
                    break
            if already_in == True:
                continue
            if len(backupable_file['backuped_bys']) == 0 and len(backupable_file['stored_bys']) == 0:
                continue
            authorizer = None
            if len(backupable_file['backuped_bys']) == 0:
                authorizer = {
                    'node_name': backupable_file['stored_bys'][-1],
                    'rank': -1,
                    'nonce': backupable_file['file_name']
                }
            else:
                authorizer = backupable_file['backuped_bys'][-1]
            # generate claim (request) msg and send
            # claim tlv
            expire_at = int(time.time()+(self.config['period']*2))
            favor = 1.85
            claim_message = ClaimMessageTlv()
            claim_message.node_name = self.config['node_name'].encode()
            claim_message.expire_at = expire_at
            claim_message.favor = str(favor).encode()
            claim_message.file_name = Name.from_str(backupable_file['file_name'])
            claim_message.type = ClaimTypes.REQUEST
            claim_message.claimer_node_name = self.config['node_name'].encode()
            claim_message.claimer_nonce = secrets.token_hex(4).encode()
            claim_message.authorizer_node_name = authorizer['node_name'].encode()
            claim_message.authorizer_nonce = authorizer['nonce'].encode()
            # claim msg
            message = Message()
            message.type = MessageTypes.CLAIM
            message.value = claim_message.encode()
            self.svs.publishData(message.encode())
            val = "[MSG][CLAIM.R]*nam={nam};fil={fil}".format(
                nam=self.config['node_name'],
                fil=backupable_file['file_name']
            )
            self.logger.info(val)


    def periodic(self):
        # print('periodic')
        # periodic tasks:
        self.heartbeat()
        self.detect_expired_nodes()
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

    def store(self, file_name: str):
        file = self.global_view.get_file(file_name)
        if len(file['stored_bys']) < file['desired_copies']:
            # store msg
            expire_at = int(time.time()+(self.config['period']*2))
            favor = 1.85
            store_message = StoreMessageTlv()
            store_message.node_name = self.config['node_name'].encode()
            store_message.expire_at = expire_at
            store_message.favor = str(favor).encode()
            store_message.file_name = Name.from_str(file_name)
            # store msg
            message = Message()
            message.type = MessageTypes.STORE
            message.value = store_message.encode()
            # apply globalview and send msg thru SVS
            # next_state_vector = svs.getCore().getStateVector().get(config['session_id']) + 1

            self.global_view.store_file(file_name, self.config['node_name'])
            self.svs.publishData(message.encode())
            val = "[MSG][STORE]*  nam={nam};fil={fil}".format(
                nam=self.config['node_name'],
                fil=file_name
            )
            self.logger.info(val)

    def svs_missing_callback(self, missing_list):
        aio.ensure_future(self.on_missing_svs_messages(missing_list))

    async def on_missing_svs_messages(self, missing_list):
        for i in missing_list:
            while i.lowSeqno <= i.highSeqno:
                # print('{}:{}, {}'.format(i.nid, i.lowSeqNum, i.highSeqNum))
                message_bytes = await self.svs.fetchData(Name.from_str(i.nid), i.lowSeqno)
                if message_bytes == None:
                    continue
                nid = i.nid
                seq = i.lowSeqno
                message = Message.specify(nid, seq, message_bytes)
                aio.ensure_future(message.apply(self.global_view, self.fetch_file, self.svs, self.config))
                # print('fetched GM {}:{}'.format(nid, seq))
                i.lowSeqno = i.lowSeqno + 1

    def svs_sending_callback(self, expire_at: int):
        self.expire_at = expire_at

    def fetch_file(self, file_name: str, packets: int, digests: List[bytes], fetch_path: str):
        val = "[ACT][FETCH]*  fil={fil};pcks={packets};fetch_path={fetch_path}".format(
            fil=file_name,
            packets=packets,
            fetch_path=fetch_path
        )
        self.logger.info(val)
        aio.ensure_future(self.async_fetch(file_name, packets, digests, fetch_path))

    async def async_fetch(self, file_name: str, packets: int, digests: List[bytes], fetch_path: str):
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
                self.store(file_name)
        elif packets == 1:
            inserted_packets = await self.fetch_single_file(file_name, fetch_path)
            if inserted_packets == packets:
                self.store(file_name)

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
            self.data_storage.put_packet(key, data_bytes)
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
        self.data_storage.put_packet(key, data_bytes)
        return 1
