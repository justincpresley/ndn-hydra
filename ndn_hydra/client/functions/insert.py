# -------------------------------------------------------------
# NDN Hydra Insert Client
# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Date:    2021-01-25
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/justincpresley/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

import logging
import time
from hashlib import blake2b
from ndn.app import NDNApp
from ndn.encoding import Name, Component, FormalName
from ndn_hydra.repo.protocol.base_models import InsertCommand, File
from ndn_hydra.repo.utils.pubsub import PubSub

SEGMENT_SIZE = 8192

class HydraInsertClient(object):
    def __init__(self, app: NDNApp, client_prefix: FormalName, repo_prefix: FormalName) -> None:
      """
      This client inserts data packets from the remote repo.
      :param app: NDNApp.
      :param client_prefix: NonStrictName. Routable name to client.
      :param repo_prefix: NonStrictName. Routable name to remote repo.
      """
      self.app = app
      self.client_prefix = client_prefix
      self.repo_prefix = repo_prefix
      self.pb = PubSub(self.app, self.client_prefix)
      self.packets = []

    async def insert_file(self, file_name: FormalName, path: str) -> bool:
      """
      Insert a file associated with a file name to the remote repo
      """
      size, seg_cnt = 0, 0
      fetch_file_prefix = self.client_prefix + [Component.from_str("upload")] + file_name
      tic = time.perf_counter()
      with open(path, "rb") as f:
        data = f.read()
        size = len(data)
        seg_cnt = (len(data) + SEGMENT_SIZE - 1) // SEGMENT_SIZE
        self.packets = [self.app.prepare_data(fetch_file_prefix + [Component.from_segment(i)],
                                              data[i*SEGMENT_SIZE:(i+1)*SEGMENT_SIZE],
                                              freshness_period=10000,
                                              final_block_id=Component.from_segment(seg_cnt - 1))
                        for i in range(seg_cnt)]

      print(f'Created {seg_cnt} chunks under name {Name.to_str(fetch_file_prefix)}')

      def on_interest(int_name, _int_param, _app_param):
        seg_no = Component.to_number(int_name[-1]) if Component.get_type(int_name[-1]) == Component.TYPE_SEGMENT else 0
        if seg_no < seg_cnt:
            self.app.put_raw_packet(self.packets[seg_no])
        if seg_no == (seg_cnt - 1):
            toc = time.perf_counter()
            print(f"The publication is complete! - total time (with disk): {toc - tic:0.4f} secs")

      self.app.route(fetch_file_prefix)(on_interest)

      file = File()
      file.file_name = file_name
      file.packets = seg_cnt
      file.packet_size = SEGMENT_SIZE
      file.size = size
      cmd = InsertCommand()
      cmd.file = file
      cmd.fetch_path = fetch_file_prefix
      cmd_bytes = cmd.encode()

      # publish msg to repo's insert topic
      await self.pb.wait_for_ready()
      is_success = await self.pb.publish(self.repo_prefix + ['insert'], cmd_bytes)
      if is_success:
          logging.debug('Published an insert msg and was acknowledged by a subscriber')
      else:
          logging.debug('Published an insert msg but was not acknowledged by a subscriber')
      return is_success
