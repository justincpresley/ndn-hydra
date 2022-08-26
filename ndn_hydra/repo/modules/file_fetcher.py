# -------------------------------------------------------------
# NDN Hydra File Fetcher
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
import random
import time
from ndn.app import NDNApp
from ndn.storage import Storage
from ndn_hydra.repo.modules import *
from ndn_hydra.repo.group_messages import *
from ndn_hydra.repo.utils.concurrent_fetcher import concurrent_fetcher

class FileFetcher:
    """
    A class to abstract client-to-node and node-to-node fetching.
    """
    def __init__(self, main_loop, app: NDNApp, global_view: GlobalView, data_storage: Storage, config: dict) -> None:
        self.main_loop = main_loop
        self.global_view = global_view
        self.app = app
        self.data_storage = data_storage
        self.config = config
        self.repo_prefix = config['repo_prefix']
        self.logger = logging.getLogger()
        self.fetching = []

    def fetch_file_from_client(self, file_name: str, packets: int, packet_size: int, fetch_path: str):
        aio.ensure_future(self._fetch_file_helper(file_name, packets, packet_size, fetch_path))
    
    def fetch_file_from_node(self, file_name: str, packets: int, packet_size: int):
        # Randomly select a node to fetch file from
        file_info = self.global_view.get_file(file_name)
        on_list = file_info["stores"]
        if file_info["is_deleted"] == True or not on_list:
            return
        active_nodes = set([node['node_name'] for node in self.global_view.get_nodes()])
        on_list = [x for x in on_list if x in active_nodes]
        selected_node = random.choice(on_list)
        if not selected_node:
            return
        # Fetch file from selected node
        fetch_path = f"{self.repo_prefix}/node/{selected_node}/fetch/{file_name}"
        aio.ensure_future(self._fetch_file_helper(file_name, packets, packet_size, fetch_path))

    async def _fetch_file_helper(self, file_name: str, packets: int, packet_size: int, fetch_path: str):
        if file_name in self.fetching:
            return
        self.fetching.append(file_name)
        self.logger.info(f"[ACT][FETCH]*   fil={file_name};pcks={packets};fetch_path={fetch_path}")
        start = time.time()
        async for (_, _, content, data_bytes, key) in concurrent_fetcher(self.app, fetch_path, file_name, 0, packets-1, aio.Semaphore(15)):
            self.data_storage.put_packet(key, data_bytes) #TODO: check digest
        end = time.time()
        duration = end - start
        self.logger.info(f"[ACT][FETCHED]* pcks={packets};duration={duration}")
        self.main_loop.store(file_name)
        try:
            self.fetching.remove(file_name)
        except:
            pass

    
