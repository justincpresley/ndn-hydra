import argparse
import asyncio as aio
import logging
import pkg_resources
import sys
from ndn.app import NDNApp
from ndn.encoding import Name
from ndn_distributed_repo import *


def process_cmd_opts():
    """
    Parse, process, and return cmd options.
    """
    def print_version():
        pkg_name = 'ndn-python-repo'
        version = pkg_resources.require(pkg_name)[0].version
        print(pkg_name + ' ' + version)

    def parse_cmd_opts():
        parser = argparse.ArgumentParser(description='ndn-python-repo')
        '''
        parser.add_argument('-v', '--version',
                            help='print current version and exit', action='store_true')
        parser.add_argument('-c', '--config',
                            help='path to config file')
        '''
        parser.add_argument('-r', '--repo_name',
                            help="""repo's routable prefix. If this option is specified, it 
                                    overrides the prefix in the config file""")
        args = parser.parse_args()
        return args

    args = parse_cmd_opts()
    """
    if args.version:
        print_version()
        exit(0)
    """
    return args

def main() -> int:
    cmdline_args = process_cmd_opts()
    config = {}
    #config = process_config(cmdline_args)
    #print(config)

    #config_logging(config['logging_config'])

    #storage = create_storage(config['db_config'])

    app = NDNApp()

    pb = PubSub(app)
    read_handle = ReadHandle(app, storage, config)
    insert_handle = InsertCommandHandle(app, None, pb, read_handle, None)
    delete_handle = DeleteCommandHandle(app, None, pb, read_handle, None)
    #tcp_bulk_insert_handle = TcpBulkInsertHandle(storage, read_handle, config)

    repo = RepoNode(app, None, read_handle, insert_handle, delete_handle, None)
    aio.ensure_future(repo.listen())

    try:
        app.run_forever()
    except FileNotFoundError:
        print('Error: could not connect to NFD.')
    return 0


if __name__ == "__main__":
    sys.exit(main())
