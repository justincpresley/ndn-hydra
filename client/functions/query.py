# -----------------------------------------------------------------------------
# NDN Distributed Repo query client.
#
# @Author Justin C Presley
# @Author Daniel Achee
# @Author Zixuan Zhong
# @Date   2021-01-25
# -----------------------------------------------------------------------------

import asyncio as aio
import logging
import os
from ndn.app import NDNApp
from ndn.encoding import FormalName, Component, Name, ContentType
from functions.utils.concurrent_fetcher import concurrent_fetcher

class QueryClient(object):
    def __init__(self, app: NDNApp, client_prefix: FormalName, repo_prefix: FormalName) -> None:
      """
      This client queries a node within the remote repo.
      :param app: NDNApp.
      :param client_prefix: NonStrictName. Routable name to client.
      :param repo_prefix: NonStrictName. Routable name to remote repo.
      """
      self.app = app
      self.client_prefix = client_prefix
      self.repo_prefix = repo_prefix

    async def produce_query(self) -> None:
      """
      Form a certain query and request that info from a node.
      """
      print("Client Query Command Failed.")