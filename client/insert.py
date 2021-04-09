# -----------------------------------------------------------------------------
# NDN Repo insert client.
#
# @Author Daniel Achee
# @Author Caton Zhong
# @Date   2021-01-25
# -----------------------------------------------------------------------------

import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))

import argparse
import asyncio as aio
from ndn_distributed_repo.protocol import RepoCommand, File, FetchPath
from ndn_distributed_repo.utils import PubSub
import logging
from ndn.app import NDNApp
from ndn.encoding import Name, Component, DecodeError, NonStrictName, FormalName
from ndn.types import InterestNack, InterestTimeout
from ndn.utils import gen_nonce
from typing import Optional


class InsertClient(object):
    def __init__(self, app: NDNApp, client_prefix: FormalName, repo_prefix: FormalName):
      """
      This client inserts data packets from the remote repo.
      :param app: NDNApp.
      :param repo_name: NonStrictName. Routable name to remote repo.
      """
      self.app = app
      self.client_prefix = client_prefix
      self.repo_prefix = repo_prefix
      self.pb = PubSub(self.app, self.client_prefix)

    async def insert_file(self, file_name: FormalName, desired_copies: int, packets: int, size: int, fetch_prefix: FormalName):
      """
      Insert file with file name file_name from repo 
      """
      # send command interest
      file = File()
      file.file_name = file_name
      file.desired_copies = desired_copies
      file.packets = packets 
      file.size = size
      fetch_path = FetchPath()
      fetch_path.prefix = fetch_prefix
      cmd = RepoCommand()
      cmd.file = file
      cmd.sequence_number = 0
      cmd.fetch_path = fetch_path
      cmd_bytes = cmd.encode()

      # publish msg to repo's insert topic
      await self.pb.wait_for_ready()
      print(Name.to_str(self.repo_prefix + ['insert']))
      is_success = await self.pb.publish(self.repo_prefix + ['insert'], cmd_bytes)
      if is_success:
          logging.info('Published an insert msg and was acknowledged by a subscriber')
      else:
          logging.info('Published an insert msg but was not acknowledged by a subscriber')
      return is_success