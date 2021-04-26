# -----------------------------------------------------------------------------
# NDN Repo insert client.
#
# @Author Justin C Presley
# @Author Daniel Achee
# @Author Caton Zhong
# @Date   2021-01-25
# -----------------------------------------------------------------------------

import asyncio
from ndn.app import NDNApp
from ndn.encoding import FormalName, Component, Name


class FetchClient(object):
    def __init__(self, app: NDNApp, client_prefix: FormalName, repo_prefix: FormalName):
      """
      This client fetches data packets from the remote repo.
      :param app: NDNApp.
      :param repo_name: NonStrictName. Routable name to remote repo.
      """
      self.app = app
      self.client_prefix = client_prefix
      self.repo_prefix = repo_prefix

    async def fetch_file(self, file_name: FormalName, path: str):
      """
      Insert a file associated with a file name to the remote repo
      """
      fetch_name = self.repo_prefix + [Component.from_str("main")] + file_name
      print("Fetching Not Implemented Yet.")