# ----------------------------------------------------------
# NDN Hydra Dump Client
# ----------------------------------------------------------
# @Project: NDN Hydra
# @Date:    2021-01-25
# @Author:  Zixuan Zhong
# @Author:  Justin C Presley
# @Author:  Daniel Achee
# @Source-Code: https://github.com/UCLA-IRL/ndn-hydra
# @Pip-Library: https://pypi.org/project/ndn-hydra/
# ----------------------------------------------------------

from time import sleep
from hydra.global_view.global_view import GlobalView
from ndn.app import NDNApp
from ndn.encoding import FormalName, Name

class DumpClient(object):
    def __init__(self, app: NDNApp, repo_prefix: FormalName, sessionid: str) -> None:
      """
      This client looks at a global view database.
      :param app: NDNApp.
      :param repo_prefix: NonStrictName. Routable name to remote repo.
      :param sessionid: str. The session ID that the node owns.
      """
      self.app = app
      self.sessionid = sessionid
      self.repo_prefix = repo_prefix
      global_view_storage = "~/.ndn/repo/{repo_prefix}/{session_id}/global_view.db".format(repo_prefix=Name.to_str(repo_prefix), session_id=sessionid)
      self.global_view = GlobalView(global_view_storage)

    def get_view(self) -> None:
      """
      Continually get the global view and print it to the terminal.
      """
      while True:
          sessions = self.global_view.get_sessions()
          node_list = []
          for index in range(len(sessions)):
              node_list.append(sessions[index]["id"])
          print(node_list)
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