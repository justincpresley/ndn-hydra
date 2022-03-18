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
      self.digests = []

    async def insert_file(self, file_name: FormalName, path: str) -> bool:
      """
      Insert a file associated with a file name to the remote repo
      """
      # send command interest

      test_name = file_name + [Component.from_version(3)] + [Component.from_segment(1)]
      size = 0
      fetch_file_prefix = self.client_prefix + [Component.from_str("upload")] + file_name

      with open(path, "rb") as f:
        data = f.read()
        size = len(data)
        seg_cnt = (len(data) + SEGMENT_SIZE - 1) // SEGMENT_SIZE
        packets = seg_cnt
        self.packets = [self.app.prepare_data(fetch_file_prefix + [Component.from_segment(i)],
                                              data[i*SEGMENT_SIZE:(i+1)*SEGMENT_SIZE],
                                              freshness_period=10000,
                                              final_block_id=Component.from_segment(seg_cnt - 1))
                        for i in range(seg_cnt)]

        self.digests = [bytes(blake2b(data[i*SEGMENT_SIZE:(i+1)*SEGMENT_SIZE]).digest()[:2]) for i in range(seg_cnt)]
        # create a manifest (filled with digests) to limit signing

      print(f'Created {seg_cnt} chunks under name {Name.to_str(fetch_file_prefix)}')

      @self.app.route(fetch_file_prefix)
      def on_interest(int_name, _int_param, _app_param):
        if Component.get_type(int_name[-1]) == Component.TYPE_SEGMENT:
            seg_no = Component.to_number(int_name[-1])
        else:
            seg_no = 0
        if seg_no < seg_cnt:
            self.app.put_raw_packet(self.packets[seg_no])

      file = File()
      file.file_name = file_name
      file.packets = packets
      file.digests = self.digests
      file.size = size
      cmd = InsertCommand()
      cmd.file = file
      cmd.fetch_path = fetch_file_prefix
      cmd_bytes = cmd.encode()
      # what if the digests dont fit? support larger files
      # look into implicit digests, FLICK

      # publish msg to repo's insert topic
      await self.pb.wait_for_ready()
      is_success = await self.pb.publish(self.repo_prefix + ['insert'], cmd_bytes)
      if is_success:
          logging.debug('Published an insert msg and was acknowledged by a subscriber')
      else:
          logging.debug('Published an insert msg but was not acknowledged by a subscriber')
      return is_success
