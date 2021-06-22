import argparse
import asyncio as aio
import logging
from ndn_distributed_repo.handle_data_storage.data_storage_handle import DataStorageHandle
from ndn_distributed_repo.data_storage.data_storage import DataStorage
from time import sleep
from typing import Dict
import pkg_resources
import sys
from threading import Thread
from ndn.app import NDNApp
from ndn.encoding import Name
from ndn_distributed_repo import *
from ndn_python_repo import SqliteStorage

def process_cmd_opts():
    """
    Parse, process, and return cmd options.
    """
    # def print_version():
    #     pkg_name = 'ndn-hydra'
    #     version = pkg_resources.require(pkg_name)[0].version
    #     print(pkg_name + ' ' + version)

    def process_prefix(input_string: str):
        if input_string[-1] == "/":
            input_string = input_string[:-1]
        if input_string[0] != "/":
            input_string = "/" + input_string
        return input_string

    def process_others(input_string: str):
        if input_string[-1] == "/":
            input_string = input_string[:-1]
        if input_string[0] == "/":
            input_string = input_string[1:]
        return input_string

    def parse_cmd_opts():
        # Command Line Parser
        parser = argparse.ArgumentParser(add_help=False,description="ndn-hydra")
        requiredArgs = parser.add_argument_group("required arguments")
        optionalArgs = parser.add_argument_group("optional arguments")
        informationArgs = parser.add_argument_group("information arguments")

        # Adding all Command Line Arguments
        requiredArgs.add_argument("-rp","--repoprefix",action="store",dest="repo_prefix",required=True,help="repo (group) prefix. Example: \"/hydra\"")
        requiredArgs.add_argument("-n", "--nodename",action="store",dest="node_name",required=True,help="node name. Example: \"node01\"")
        requiredArgs.add_argument("-s", "--sessionid",action="store",dest="session_id",required=True,help="id of this session. Example: \"2c4f\"")

        # Getting all Arguments
        vars = parser.parse_args()
        args = {}

        # Process args
        args["repo_prefix"] = process_prefix(vars.repo_prefix)
        args["node_name"] = process_others(vars.node_name)
        args["session_id"] = process_others(vars.session_id)
        args["data_storage_path"] = "~/.ndn/repo{repo_prefix}/{session_id}/data.db".format(repo_prefix=args["repo_prefix"], session_id=args["session_id"])
        args["global_view_path"] = "~/.ndn/repo{repo_prefix}/{session_id}/global_view.db".format(repo_prefix=args["repo_prefix"], session_id=args["session_id"])
        args["svs_storage_path"] = "~/.ndn/repo{repo_prefix}/{session_id}/svs.db".format(repo_prefix=args["repo_prefix"], session_id=args["session_id"])

        return args

    args = parse_cmd_opts()
    """
    if args.version:
        print_version()
        exit(0)
    """
    return args

async def listen(repo_prefix: Name, pb: PubSub, insert_handle: InsertCommandHandle, delete_handle: DeleteCommandHandle):
    # pubsub
    pb.set_publisher_prefix(repo_prefix)
    await pb.wait_for_ready()
    # protocol handle
    await insert_handle.listen(repo_prefix)
    await delete_handle.listen(repo_prefix)

class HydraSessionThread(Thread):
    def __init__(self, config: Dict):
        Thread.__init__(self)
        self.config = config

    def run(self) -> None:
        loop = aio.new_event_loop()
        aio.set_event_loop(loop)
        app = NDNApp()

        # data_storage = SqliteStorage(self.config['data_storage_path']+"abc.db")
        data_storage = SqliteStorage(self.config['data_storage_path'])
        global_view = GlobalView(self.config['global_view_path'])
        pb = PubSub(app)

        # main_loop (svs)
        main_loop = MainLoop(app, self.config, global_view, data_storage)

        # protocol (reads, commands & queries)
        read_handle = ReadHandle(app, data_storage, global_view, self.config)
        insert_handle = InsertCommandHandle(app, data_storage, pb, self.config, main_loop, global_view)
        delete_handle = DeleteCommandHandle(app, data_storage, pb, self.config, main_loop, global_view)
        query_handle = QueryHandle(app, global_view, self.config)

        # start listening
        aio.ensure_future(listen(Name.normalize(self.config['repo_prefix']), pb, insert_handle, delete_handle))

        try:
            app.run_forever(after_start=main_loop.start())
        except FileNotFoundError:
            print('Error: could not connect to NFD.')

# class FileFetchingThread(Thread):
#     def __init__(self, config: Dict):
#         Thread.__init__(self)
#         self.config = config

#     def run(self) -> None:
#         loop = aio.new_event_loop()
#         aio.set_event_loop(loop)

#         app = NDNApp()
#         data_storage = DataStorage(self.config['data_storage_path'])
#         data_storage_handle = DataStorageHandle(app, self.config, data_storage)

#         try:
#             app.run_forever(after_start=data_storage_handle.start())
#         except FileNotFoundError:
#             print('Error: could not connect to NFD.')



def main() -> int:
    default_config = {
        'repo_prefix': None,
        'node_name': None,
        'session_id': None,
        'data_storage_path': None,
        'global_view_path': None,
        'svs_storage_path': None,
        #'svs_cache_others': True,
		'period': 5
    }
    cmd_args = process_cmd_opts()
    config = default_config.copy()
    config.update(cmd_args)

    HydraSessionThread(config).start()
    return 0


if __name__ == "__main__":
    sys.exit(main())
