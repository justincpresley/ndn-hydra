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
from ndn.types import InterestNack, InterestTimeout, InterestCanceled, ValidationFailure
from ndn_distributed_repo.protocol import File

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

    async def send_query(self, query: Name, sid: str=None) -> None:
      """
      Form a certain query and request that info from a node.
      """
      if not sid:
          named_query = self.repo_prefix + [Component.from_str("query")] + query
      else:
          named_query = self.repo_prefix + [Component.from_str("sid-query")] + [Component.from_str(sid)] + query

      try:
          data_name, meta_info, content = await self.app.express_interest(named_query, can_be_prefix=True, must_be_fresh=True, lifetime=3000)
          if meta_info.content_type == ContentType.NACK:
             print("Distributed Repo does not know that query.")
             return
          else:
             querytype = Component.to_str(query[0])
             if querytype == "sids":
                 print(f'List of All Session IDs')
                 print(f'{bytes(content).decode()}')
                 return
      except (InterestNack, InterestTimeout, InterestCanceled, ValidationFailure) as e:
          print("Query command received no data packet back")
          return