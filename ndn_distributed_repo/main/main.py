import argparse
import asyncio as aio
import logging
import pkg_resources
import sys
from ndn.app import NDNApp
from ndn.encoding import Name
from ndn.security import KeychainSqlite3, TpmFile
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
        parser = argparse.ArgumentParser(add_help=False,description="ndn-distributed-repo")
        requiredArgs = parser.add_argument_group("required arguments")
        optionalArgs = parser.add_argument_group("optional arguments")
        informationArgs = parser.add_argument_group("information arguments")

        # Adding all Command Line Arguments
        requiredArgs.add_argument("-rp","--repoprefix",action="store",dest="repo_prefix",required=True,help="repo (group) prefix. Example: \"/samplerepo\"")
        requiredArgs.add_argument("-gp", "--svsgroupprefix",action="store",dest="svs_group_prefix",required=True,help="prefix of svs group. Example: \"/repogroup\"")
        requiredArgs.add_argument("-n", "--nodename",action="store",dest="node_name",required=True,help="node name. Example: \"node01\"")
        requiredArgs.add_argument("-s", "--sessionid",action="store",dest="session_id",required=True,help="id of this session. Example: \"2c4f\"")

        # Getting all Arguments
        vars = parser.parse_args()
        args = {}

        # Process args
        args["repo_prefix"] = process_prefix(vars.repo_prefix)
        args["node_name"] = process_others(vars.node_name)
        args["session_id"] = process_others(vars.session_id)
        args["file_storage"] = "~/.ndn/repo/{repo_prefix}/{session_id}/file.db".format(repo_prefix=args["repo_prefix"], session_id=args["session_id"])
        args["global_view_storage"] = "~/.ndn/repo/{repo_prefix}/{session_id}/global_view.db".format(repo_prefix=args["repo_prefix"], session_id=args["session_id"])
        args["svs_storage"] = "~/.ndn/repo/{repo_prefix}/{session_id}/svs.db".format(repo_prefix=args["repo_prefix"], session_id=args["session_id"])
        args["svs_group_prefix"] = process_prefix(vars.svs_group_prefix)
        
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


def main() -> int:

    default_config = {
        'repo_prefix': None,
        'node_name': None,
        'session_id': None,
        'file_storage': None,
        'global_view_storage': None,
        'svs_storage': None,
        'svs_group_prefix': None,
        'svs_cache_others': False
    }
    cmd_args = process_cmd_opts()
    config = default_config.copy()
    config.update(cmd_args)
    #config = process_config(cmdline_args)
    #print(config)

    # config_logging(config['logging_config'])

    # storage = create_storage(config['db_config'])

    app = NDNApp()
    # tpm = TpmFile("/home/zixuan/.ndn/ndnsec-key-file")
    # keychain = KeychainSqlite3("/home/zixuan/.ndn/pib.db", tpm)
    # if keychain.has_default_identity():
    #     print("yes")
    # else:
    #     print("no")
    #     keychain.touch_identity(Name.from_str("/example_identity"))


    file_storage = SqliteStorage(config['file_storage'])
    global_view = GlobalView(config['global_view_storage'])

    # messages (svs) 
    message_handle = MessageHandle(app, SqliteStorage(config['svs_storage']), config['svs_group_prefix'], config['session_id'], config['node_name'], config['svs_cache_others'], global_view, config)

    # protocol (commands & queries)
    pb = PubSub(app)
    read_handle = ReadHandle(app, file_storage, config)
    insert_handle = InsertCommandHandle(app, file_storage, pb, read_handle, config, message_handle, global_view)
    delete_handle = DeleteCommandHandle(app, file_storage, pb, read_handle, config)

    # repo = RepoNode(app, None, read_handle, insert_handle, delete_handle, config)
    # aio.ensure_future(repo.listen())
    
    # listen
    repo_prefix = Name.from_str(config['repo_prefix'])
    aio.ensure_future(listen(repo_prefix, pb, insert_handle, delete_handle))
    # # pubsub
    # pb.set_publisher_prefix(repo_prefix)
    # await pb.wait_for_ready()
    # # protocol handle
    # await insert_handle.listen(repo_prefix)
    # await delete_handle.listen(repo_prefix)


    try:
        app.run_forever(after_start=message_handle.start())
    except FileNotFoundError:
        print('Error: could not connect to NFD.')
    return 0


if __name__ == "__main__":
    sys.exit(main())
