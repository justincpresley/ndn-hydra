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

      self.normal_serving_comp = "/query"
      self.personal_serving_comp = "/sid-query"

    async def send_query(self, query: Name, sid: str=None) -> None:
      """
      Form a certain query and request that info from a node.
      """
      named_query = self.repo_prefix
      if not sid:
          named_query = named_query + [Component.from_str(self.normal_serving_comp)] + query
      else:
          named_query = named_query + [Component.from_str(self.personal_serving_comp)] + [Component.from_str(self.sid)] + query

      data_name, meta_info, content, data_bytes = await self.app.express_interest(named_query,
                                                        can_be_prefix=True, must_be_fresh=True, lifetime=1000)
      if meta_info.content_type == ContentType.NACK:
        print("Distributed Repo does not know that query.")
        return
      else:
        return