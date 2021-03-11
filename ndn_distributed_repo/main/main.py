import argparse
import asyncio as aio
import logging
import pkg_resources
import sys
from ndn.app import NDNApp
from ndn.encoding import Name
from ndn_distributed_repo import *
from ndn_python_repo import SqliteStorage
# from handle_messages import MessageHandle
# from . import *


def process_cmd_opts():
    """
    Parse, process, and return cmd options.
    """
    # def print_version():
    #     pkg_name = 'ndn-distributed-repo'
    #     version = pkg_resources.require(pkg_name)[0].version
    #     print(pkg_name + ' ' + version)

    def parse_cmd_opts():
        # Command Line Parser
        parser = argparse.ArgumentParser(add_help=False,description="ndn-distributed-repo")
        requiredArgs = parser.add_argument_group("required arguments")
        optionalArgs = parser.add_argument_group("optional arguments")
        informationArgs = parser.add_argument_group("information arguments")
        # Adding all Command Line Arguments
        requiredArgs.add_argument("-n", "--nodeid",action="store",dest="node_id",required=True,help="id of this repo node. Example: \"node01\"")
        requiredArgs.add_argument("-rp","--repoprefix",action="store",dest="repo_prefix",required=True,help="repo (group) prefix. Example: \"/sample/repo\"")
        # Getting all Arguments
        vars = parser.parse_args()
        args = {}
        # Process args
        if vars.repo_prefix[-1] == "/":
            vars.repo_prefix = vars.repo_prefix[:-1]
        if vars.repo_prefix[0] != "/":
            vars.repo_prefix = "/" + vars.repo_prefix
        args["repo_prefix"] = vars.repo_prefix
        if vars.node_id[-1] == "/":
            vars.node_id = vars.node_id[:-1]
        if vars.node_id[0] != "/":
            vars.node_id = "/" + vars.node_id
        args["node_id"] = vars.node_id
        return args

    args = parse_cmd_opts()
    """
    if args.version:
        print_version()
        exit(0)
    """
    return args

def main() -> int:

    default_config = {
        'node_id': None,
        'repo_prefix': None,
        'file_storage_path': '~/.ndn/repo/file.db',
        'svs': {
            'cache_others': False,
            'group_prefix': '/group',
            'storage_path': '~/.ndn/repo/svs.db',
        }
    }
    cmd_args = process_cmd_opts()
    config = default_config.copy()
    config.update(cmd_args)
    #config = process_config(cmdline_args)
    #print(config)

    # config_logging(config['logging_config'])

    # storage = create_storage(config['db_config'])

    app = NDNApp()

    svs_storage = SqliteStorage(config['svs']['storage_path'])
    file_storage = None
    global_view = GlobalView()

    # svs
    message_handle = MessageHandle(app, svs_storage, config['svs']['group_prefix'], config['node_id'], config['svs']['cache_others'])

    # protocol (commands & queries)
    pb = PubSub(app)
    read_handle = ReadHandle(app, file_storage, config)
    insert_handle = InsertCommandHandle(app, file_storage, pb, read_handle, config, message_handle)
    delete_handle = DeleteCommandHandle(app, file_storage, pb, read_handle, config)

    repo = RepoNode(app, None, read_handle, insert_handle, delete_handle, config)
    aio.ensure_future(repo.listen())

    try:
        app.run_forever(after_start=message_handle.start())
    except FileNotFoundError:
        print('Error: could not connect to NFD.')
    return 0


if __name__ == "__main__":
    sys.exit(main())
