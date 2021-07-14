# ----------------------------------------------------------
# NDN Hydra Client
# ----------------------------------------------------------
# @Project: NDN Hydra
# @Date:    2021-01-25
# @Author:  Zixuan Zhong
# @Author:  Justin C Presley
# @Author:  Daniel Achee
# @Source-Code: https://github.com/UCLA-IRL/ndn-hydra
# @Pip-Library: https://pypi.org/project/ndn-hydra/
# ----------------------------------------------------------

import asyncio
from argparse import ArgumentParser, Namespace
import logging
from ndn.app import NDNApp
from ndn.encoding import Name, Component, FormalName
import sys, os
from ndn_hydra.client.functions import *

def parse_hydra_cmd_opts() -> Namespace:
    # Command Line Parser
    parser = ArgumentParser(description="A Distributed Repo Client")
    subparsers = parser.add_subparsers(title="Client Commands", dest="function")
    subparsers.required = True

    # Define All Subparsers
    insertsp = subparsers.add_parser('insert')
    insertsp.add_argument("-r","--repoprefix",action="store",dest="repo",required=True, help="A proper Name of The Repo Prefix.")
    insertsp.add_argument("-f","--filename",action="store",dest="filename",required=True, help="A proper Name for the file.")
    insertsp.add_argument("-p","--path",action="store",dest="path",required=True, help="The path of the file desired to be the input.")

    deletesp = subparsers.add_parser('delete')
    deletesp.add_argument("-r","--repoprefix",action="store",dest="repo",required=True, help="A proper Name of The Repo Prefix.")
    deletesp.add_argument("-f","--filename",action="store",dest="filename",required=True, help="A proper Name for the file.")

    fetchsp = subparsers.add_parser('fetch')
    fetchsp.add_argument("-r","--repoprefix",action="store",dest="repo",required=True, help="A proper Name of The Repo Prefix.")
    fetchsp.add_argument("-f","--filename",action="store",dest="filename",required=True, help="A proper Name for the file.")
    fetchsp.add_argument("-p","--path",action="store",dest="path",default="./client/example/fetchedFile", required=False, help="The path you want the file to be placed.")

    querysp = subparsers.add_parser('query')
    querysp.add_argument("-r","--repoprefix",action="store",dest="repo",required=True, help="A proper Name of The Repo Prefix.")
    querysp.add_argument("-s","--sessionid",action="store",dest="sessionid",default=None, required=False, help="The session ID of the node you want to query. Best-route by default.")
    querysp.add_argument("-q","--query",action="store",dest="query",required=True, help="The Query you want to send to the Repo.")

    # Getting all Arguments
    vars = parser.parse_args()

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
    async def insert(self, file_name: FormalName, desired_copies: int, path: str) -> bool:
        return await self.cinsert.insert_file(file_name, desired_copies, path);
    async def delete(self, file_name: FormalName) -> bool:
        return await self.cdelete.delete_file(file_name);
    async def fetch(self, file_name: FormalName, local_filename: str = None, overwrite: bool = False) -> None:
        return await self.cfetch.fetch_file(file_name, local_filename, overwrite)
    async def query(self, query: Name, sid: str=None) -> None:
        return await self.cquery.send_query(query, sid)

async def run_hydra_client(app: NDNApp, args: Namespace) -> None:
  repo_prefix = Name.from_str(args.repo)
  client_prefix = Name.from_str("/client")
  filename = None
  desired_copies = 2
  client = HydraClient(app, client_prefix, repo_prefix)

  if args.function != "query":
      filename = Name.from_str(args.filename)

  if args.function == "insert":
    await client.insert(filename, desired_copies, args.path)
    print("Client finished Insert Command!")
    await asyncio.sleep(60)

  elif args.function == "delete":
    await client.delete(filename)
    print("Client finished Delete Command!")

  elif args.function == "fetch":
    await client.fetch(filename, args.path, True)
    print("Client finished Fetch Command!")

  elif args.function == "query":
    await client.query(Name.from_str(str(args.query)), args.sessionid)
    print("Client finished Query Command!")

  else:
    print("Not Implemented Yet / Unknown Command.")

  app.shutdown()

def main() -> None:
    args = parse_hydra_cmd_opts()
    app = NDNApp()
    try:
        app.run_forever(after_start=run_hydra_client(app, args))
    except FileNotFoundError:
        print('Error: could not connect to NFD.')
        sys.exit()

if __name__ == "__main__":
    sys.exit(main())
