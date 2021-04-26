# -----------------------------------------------------------------------------
# NDN Repo insert client.
#
# @Author Justin C Presley
# @Author Daniel Achee
# @Author Caton Zhong
# @Date   2021-01-25
# -----------------------------------------------------------------------------

import asyncio as aio
import logging
import os
from ndn.app import NDNApp
from ndn.encoding import FormalName, Component, Name
from functions.utils.concurrent_fetcher import concurrent_fetcher

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

    async def fetch_file(self, file_name: FormalName, local_filename: str = None, overwrite: bool = False):
      """
      Fetch a file from remote repo, and write to the current working directory.
      :param name_at_repo: NonStrictName. The name with which this file is stored in the repo.
      :param local_filename: str. The filename of the retrieved file on the local file system.
      :param overwrite: If true, existing files are replaced.
      """
      name_at_repo = self.repo_prefix + [Component.from_str("main")] + file_name

      # If no local filename is provided, store file with last name component
      # of repo filename
      if local_filename is None:
        local_filename = Name.to_str(file_name)
        local_filename = os.path.basename(local_filename)

      # If the file already exists locally and overwrite=False, retrieving the file makes no
      # sense.
      if os.path.isfile(local_filename) and not overwrite:
        raise FileExistsError("{} already exists".format(local_filename))

      # Fetch the file.
      semaphore = aio.Semaphore(10)
      b_array = bytearray()
      async for (_, _, content, _) in concurrent_fetcher(self.app, name_at_repo, 0, None, semaphore):
        b_array.extend(content)

      # After b_array is filled, sort out what to do with the data.
      if len(b_array) > 0:
        logging.info(f'Fetching completed, writing to file {local_filename}')

        # Create folder hierarchy
        local_folder = os.path.dirname(local_filename)
        if local_folder:
          os.makedirs(local_folder, exist_ok=True)

        # Write retrieved data to file
        if os.path.isfile(local_filename) and overwrite:
          os.remove(local_filename)
        with open(local_filename, 'wb') as f:
          f.write(b_array)
      else:
        print("Client Fetch Command Failed.")