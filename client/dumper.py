from time import sleep
from ndn_distributed_repo.global_view_2.global_view import GlobalView
import sys

repo_prefix = sys.argv[1]
session_id = sys.argv[2]

global_view_storage = "~/.ndn/repo/{repo_prefix}/{session_id}/global_view.db".format(repo_prefix=repo_prefix, session_id=session_id)
global_view = GlobalView(global_view_storage)

while True:
    insertions = global_view.get_insertions()
    for insertion in insertions:
        on = ""
        for stored_by in insertion['stored_bys']:
            on = on + stored_by + ","
        bck = ""
        for backuped_by in insertion['backuped_bys']:
            bck = bck + backuped_by['session_id'] + ","
        val = '[File]           iid={iid}; name={name}; on={on}; bck={bck}'.format(
            iid=insertion['id'],
            name=insertion['file_name'],
            on=on,
            bck=bck
        )
        print(val)
    print("--")
    sleep(3)