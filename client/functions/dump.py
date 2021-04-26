# -----------------------------------------------------------------------------
# NDN Repo insert client.
#
# @Author Justin C Presley
# @Author Daniel Achee
# @Author Caton Zhong
# @Date   2021-01-25
# -----------------------------------------------------------------------------

from time import sleep
from ndn_distributed_repo.global_view_2.global_view import GlobalView
from ndn.app import NDNApp
from ndn.encoding import FormalName

class DumpClient(object):
    def __init__(self, app: NDNApp, repo_prefix: FormalName, sessionid: str):
      """
      This client fetches data packets from the remote repo.
      :param app: NDNApp.
      :param repo_name: NonStrictName. Routable name to remote repo.
      """
      self.app = app
      self.sessionid = sessionid
      self.repo_prefix = repo_prefix
      global_view_storage = "~/.ndn/repo/{repo_prefix}/{session_id}/global_view.db".format(repo_prefix=repo_prefix, session_id=sessionid)
      self.global_view = GlobalView(global_view_storage)

    def get_view(self):
      while True:
          insertions = self.global_view.get_insertions()
          for insertion in insertions:
              on = ""
              for stored_by in insertion['stored_bys']:
                  on = on + stored_by + ","
              bck = ""
              for backuped_by in insertion['backuped_bys']:
                  bck = bck + backuped_by['session_id'] + ","
              val = 'iid={iid}; name={name}; on={on}; bck={bck}'.format(
                  iid=insertion['id'],
                  name=insertion['file_name'],
                  on=on,
                  bck=bck
              )
              print(val)
          print("--")
          sleep(3)