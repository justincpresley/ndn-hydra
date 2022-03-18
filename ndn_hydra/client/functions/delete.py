# -------------------------------------------------------------
# NDN Hydra Delete Client
# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Date:    2021-01-25
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/justincpresley/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

import logging
from ndn.app import NDNApp
from ndn.encoding import Name, Component, FormalName
from ndn_hydra.repo.protocol.base_models import DeleteCommand
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
      cmd = DeleteCommand()
      cmd.file_name = file_name
      cmd_bytes = cmd.encode()

      # publish msg to repo's delete topic
      await self.pb.wait_for_ready()
      is_success = await self.pb.publish(self.repo_prefix + ['delete'], cmd_bytes)
      if is_success:
          logging.debug('Published an delete msg and was acknowledged by a subscriber')
      else:
          logging.debug('Published an delete msg but was not acknowledged by a subscriber')
      return is_success
