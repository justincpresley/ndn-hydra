# -------------------------------------------------------------
# NDN Hydra Client
# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Date:    2021-01-25
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/justincpresley/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

import asyncio
from argparse import ArgumentParser, Namespace
import logging
from ndn.app import NDNApp
from ndn.encoding import Name, FormalName
import sys
import os
import time
import pkg_resources
from ndn_hydra.client.functions import *

def parse_hydra_cmd_opts() -> Namespace:
    def interpret_version() -> None:
        set = True if "-v" in sys.argv else False
        if set and (len(sys.argv)-1 < 2):
            try: print("ndn-hydra " + pkg_resources.require("ndn-hydra")[0].version)
            except pkg_resources.DistributionNotFound: print("ndn-hydra source,undetermined")
            sys.exit(0)
    def interpret_help() -> None:
        set = True if "-h" in sys.argv else False
        if set:
            if (len(sys.argv)-1 < 2):
                print("usage: ndn-hydra-client [-h] [-v] {insert,delete,fetch,query} ...")
                print("    ndn-hydra-client: a client made specifically for hydra, the NDN distributed repo.")
                print("    ('python3 ./examples/client.py' instead of 'ndn-hydra-client' if from source.)")
                print("")
                print("* informational args:")
                print("  -h, --help                      |   shows this help message and exits.")
                print("  -v, --version                   |   shows the current version and exits.")
                print("")
                print("* function 'insert':")
                print("     usage: ndn-hydra-client insert -r REPO -f FILENAME -p PATH [-c COPIES]")
                print("     required args:")
                print("        -r, --repoprefix REPO     |   a proper name of the repo prefix.")
                print("        -f, --filename FILENAME   |   a proper name for the input file.")
                print("        -p, --path PATH           |   path of the file desired to be the input i.e. input path.")
                print("")
                print("* function 'delete':")
                print("     usage: ndn-hydra-client delete -r REPO -f FILENAME")
                print("     required args:")
                print("        -r, --repoprefix REPO     |   a proper name of the repo prefix.")
                print("        -f, --filename FILENAME   |   a proper name for selected file.")
                print("")
                print("* function 'fetch':")
                print("     usage: ndn-hydra-client fetch -r REPO -f FILENAME [-p PATH]")
                print("     required args:")
                print("        -r, --repoprefix REPO     |   a proper name of the repo prefix.")
                print("        -f, --filename FILENAME   |   a proper name for desired file.")
                print("     optional args:")
                print("        -p, --path PATH           |   path for the file to be placed i.e. output path.")
                print("")
                print("* function 'query':")
                print("     usage: ndn-hydra-client query -r REPO -q QUERY [-s SESSIONID]")
                print("     required args:")
                print("        -r, --repoprefix REPO     |   a proper name of the repo prefix.")
                print("        -q, --query QUERY         |   the type of query desired.")
                print("     optional args:")
                print("        -s, --sessionid SESSIONID |   certain sessionid-node targeted for query, default closest node.")
                print("")
                print("Thank you for using hydra.")
            sys.exit(0)
    # Command Line Parser
    parser = ArgumentParser(prog="ndn-hydra-client",add_help=False,allow_abbrev=False)
    parser.add_argument("-h","--help",action="store_true",dest="help",default=False,required=False)
    parser.add_argument("-v","--version",action="store_true",dest="version",default=False,required=False)
    subparsers = parser.add_subparsers(dest="function",required=True)

    # Define All Subparsers
    insertsp = subparsers.add_parser('insert',add_help=False)
    insertsp.add_argument("-r","--repoprefix",action="store",dest="repo",required=True)
    insertsp.add_argument("-f","--filename",action="store",dest="filename",required=True)
    insertsp.add_argument("-p","--path",action="store",dest="path",required=True)
    insertsp.add_argument("-w","--wait",action="store",dest="wait",required=True)

    deletesp = subparsers.add_parser('delete',add_help=False)
    deletesp.add_argument("-r","--repoprefix",action="store",dest="repo",required=True)
    deletesp.add_argument("-f","--filename",action="store",dest="filename",required=True)

    fetchsp = subparsers.add_parser('fetch',add_help=False)
    fetchsp.add_argument("-r","--repoprefix",action="store",dest="repo",required=True)
    fetchsp.add_argument("-f","--filename",action="store",dest="filename",required=True)
    fetchsp.add_argument("-p","--path",action="store",dest="path",default="./fetchedHydraFile", required=False)

    querysp = subparsers.add_parser('query',add_help=False)
    querysp.add_argument("-r","--repoprefix",action="store",dest="repo",required=True)
    querysp.add_argument("-q","--query",action="store",dest="query",required=True)
    querysp.add_argument("-n","--nodename",action="store",dest="nodename",default=None, required=False)

    # Interpret Informational Arguments
    interpret_version()
    interpret_help()

    # Getting all Arguments
    vars = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    # Configure Arguments
    if vars.function == "insert":
        if not os.path.isfile(vars.path):
            print('Error: path specified is not an actual file. Unable to insert.')
            sys.exit()
    return vars

class HydraClient():
    def __init__(self, app: NDNApp, client_prefix: FormalName, repo_prefix: FormalName) -> None:
        self.cinsert = HydraInsertClient(app, client_prefix, repo_prefix)
        self.cdelete = HydraDeleteClient(app, client_prefix, repo_prefix)
        self.cfetch = HydraFetchClient(app, client_prefix, repo_prefix)
        self.cquery = HydraQueryClient(app, client_prefix, repo_prefix)
    async def insert(self, file_name: FormalName, path: str) -> bool:
        return await self.cinsert.insert_file(file_name, path);
    async def delete(self, file_name: FormalName) -> bool:
        return await self.cdelete.delete_file(file_name);
    async def fetch(self, file_name: FormalName, local_filename: str = None, overwrite: bool = False) -> None:
        return await self.cfetch.fetch_file(file_name, local_filename, overwrite)
    async def query(self, query: Name, node_name: str=None) -> None:
        return await self.cquery.send_query(query, node_name)

async def run_hydra_client(app: NDNApp, args: Namespace) -> None:
  repo_prefix = Name.from_str(args.repo)
  client_prefix = Name.from_str("/client")
  filename = None
  client = HydraClient(app, client_prefix, repo_prefix)

  if args.function != "query":
      filename = Name.from_str(args.filename)

  if args.function == "insert":
    await client.insert(filename, args.path)
    print("Client finished Insert Command!")
    await asyncio.sleep(float(args.wait))
  elif args.function == "delete":
    tic = time.perf_counter()
    await client.delete(filename)
    toc = time.perf_counter()
    print("Client finished Delete Command! - total time (with disk): {toc - tic:0.4f} secs")
  elif args.function == "fetch":
    tic = time.perf_counter()
    await client.fetch(filename, args.path, True)
    toc = time.perf_counter()
    print(f"Client finished Fetch Command! - total time (with disk): {toc - tic:0.4f} secs")
  elif args.function == "query":
    tic = time.perf_counter()
    await client.query(Name.from_str(str(args.query)), args.nodename)
    toc = time.perf_counter()
    print(f"Client finished Query Command! - total time (with disk): {toc - tic:0.4f} secs")
  else:
    print("Not Implemented Yet / Unknown Command.")

  app.shutdown()

def main() -> None:
    args = parse_hydra_cmd_opts()
    app = NDNApp()
    try:
        app.run_forever(after_start=run_hydra_client(app, args))
    except (FileNotFoundError, ConnectionRefusedError):
        print('Error: could not connect to NFD.')
        sys.exit()

if __name__ == "__main__":
    sys.exit(main())
