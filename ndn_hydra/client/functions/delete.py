# ----------------------------------------------------------
# NDN Hydra Delete Client
# ----------------------------------------------------------
# @Project: NDN Hydra
# @Date:    2021-01-25
# @Author:  Zixuan Zhong
# @Author:  Justin C Presley
# @Author:  Daniel Achee
# @Source-Code: https://github.com/UCLA-IRL/ndn-hydra
# @Pip-Library: https://pypi.org/project/ndn-hydra/
# ----------------------------------------------------------

import asyncio
import logging
from ndn.app import NDNApp
from ndn.encoding import Name, Component, FormalName
from ndn_hydra.repo.protocol.repo_commands import RepoCommand, File, FetchPath
from ndn_hydra.repo.utils.pubsub import PubSub

class HydraDeleteClient(object):
    def __init__(self, app: NDNApp, client_prefix: FormalName, repo_prefix: FormalName) -> None:
      """
      This client deletes a certain file from the remote repo.
      :param app: NDNApp.
      :param client_prefix: NonStrictName. Routable name to client.
      :param repo_prefix: NonStrictName. Routable name to remote repo.
      """
      self.app = app
      self.client_prefix = client_prefix
      self.repo_prefix = repo_prefix
      self.pb = PubSub(self.app, self.client_prefix)

    async def delete_file(self, file_name: FormalName) -> bool:
      """
      Delete a file asscoiated with a file name from the remote repo
      """
      # send command interest
      file = File()
      file.file_name = file_name
      file.desired_copies = 0
      file.packets = 0
      file.digests = []
      file.size = 0
      fetch_path = FetchPath()
      fetch_path.prefix = self.client_prefix + [Component.from_str("upload")] + file_name
      cmd = RepoCommand()
      cmd.file = file
      cmd.sequence_number = 0
      cmd.fetch_path = fetch_path
      cmd_bytes = cmd.encode()

      # publish msg to repo's delete topic
      await self.pb.wait_for_ready()
      print(Name.to_str(self.repo_prefix + ['delete']))
      is_success = await self.pb.publish(self.repo_prefix + ['delete'], cmd_bytes)
      if is_success:
          logging.info('Published an delete msg and was acknowledged by a subscriber')
      else:
          logging.info('Published an delete msg but was not acknowledged by a subscriber')
      return is_success