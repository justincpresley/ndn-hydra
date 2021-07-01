# ----------------------------------------------------------
# NDN Hydra Main
# ----------------------------------------------------------
# @Project: NDN Hydra
# @Date:    2021-01-25
# @Author:  Zixuan Zhong
# @Author:  Justin C Presley
# @Author:  Daniel Achee
# @Source-Code: https://github.com/UCLA-IRL/ndn-hydra
# @Pip-Library: https://pypi.org/project/ndn-hydra/
# ----------------------------------------------------------

import argparse
import asyncio as aio
import logging
import os
from typing import Dict
import sys
from threading import Thread
from ndn.app import NDNApp
from ndn.encoding import Name
from ndn_python_repo import SqliteStorage
from ndn_hydra import *


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
        workpath = "{home}/.ndn/repo{repo_prefix}/{session_id}".format(home=os.path.expanduser("~"), repo_prefix=args["repo_prefix"], session_id=args["session_id"])
        args["logging_path"] = "{workpath}/session.log".format(workpath=workpath)
        args["data_storage_path"] = "{workpath}/data.db".format(workpath=workpath)
        args["global_view_path"] = "{workpath}/global_view.db".format(workpath=workpath)
        args["svs_storage_path"] = "{workpath}/svs.db".format(workpath=workpath)

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

        if len(os.path.dirname(self.config['logging_path'])) > 0 and not os.path.exists(os.path.dirname(self.config['logging_path'])):
            try:
                os.makedirs(os.path.dirname(self.config['logging_path']))
            except PermissionError:
                raise PermissionError("Could not create directory: {}".format(self.config['logging_path'])) from None
            except FileExistsError:
                pass

        logging.basicConfig(level=logging.INFO,
                            format='%(created)f  %(levelname)-8s  %(message)s',
                            filename=self.config['logging_path'],
                            filemode='w')

        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        # logging.getLogger().addHandler(console)


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


def main() -> int:
    default_config = {
        'repo_prefix': None,
        'node_name': None,
        'session_id': None,
        'data_storage_path': None,
        'global_view_path': None,
        'svs_storage_path': None,
        'logging_path': None,
        #'svs_cache_others': True,
		'period': 20
    }
    cmd_args = process_cmd_opts()
    config = default_config.copy()
    config.update(cmd_args)

    HydraSessionThread(config).start()
    return 0


if __name__ == "__main__":
    sys.exit(main())