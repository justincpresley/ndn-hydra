# -----------------------------------------------------------------------------
# NDN Distributed Repo client.
#
# @Author Justin C Presley
# @Author Daniel Achee
# @Author Zixuan Zhong
# @Date   2021-01-25
# -----------------------------------------------------------------------------

import asyncio
from argparse import ArgumentParser, Namespace
import sys
import logging
from os import path
from ndn.app import NDNApp
from ndn.encoding import Name

from functions.insert import InsertClient
from functions.delete import DeleteClient
from functions.fetch import FetchClient
from functions.query import QueryClient
from functions.dump import DumpClient

def parse_cmd_opts() -> Namespace:
    # Command Line Parser
    parser = ArgumentParser(description="A Distributed Repo Client")
    subparsers = parser.add_subparsers(title="Client Commands", dest="function")
    subparsers.required = True

    # Define All Subparsers
    insertsp = subparsers.add_parser('insert')
    insertsp.add_argument("-f","--filename",action="store",dest="filename",required=True, help="A proper Name for the file.")
    insertsp.add_argument("-p","--path",action="store",dest="path",required=True, help="The path of the file desired to be the input.")

    deletesp = subparsers.add_parser('delete')
    deletesp.add_argument("-f","--filename",action="store",dest="filename",required=True, help="A proper Name for the file.")

    fetchsp = subparsers.add_parser('fetch')
    fetchsp.add_argument("-f","--filename",action="store",dest="filename",required=True, help="A proper Name for the file.")
    fetchsp.add_argument("-p","--path",action="store",dest="path",default="./client/example/fetchedFile", required=False, help="The path you want the file to be placed.")

    querysp = subparsers.add_parser('query')
    querysp.add_argument("-s","--sessionid",action="store",dest="sessionid",default=None, required=False, help="The session ID of the node you want to query. Best-route by default.")
    querysp.add_argument("-q","--query",action="store",dest="query",required=True, help="The Query you want to send to the Repo.")

    dumpsp = subparsers.add_parser('dump')
    dumpsp.add_argument("-s","--sessionid",action="store",dest="sessionid",required=True, help="The session ID of the node.")

    # Getting all Arguments
    vars = parser.parse_args()

    # Configure Arguments
    if vars.function == "insert":
        if not path.isfile(vars.path):
          print('Error: path specified is not an actual file. Unable to insert.')
          sys.exit()
    return vars

async def run_client(app: NDNApp, args: Namespace) -> None:
  repo_prefix = Name.from_str("/pndrepo")
  client_prefix = Name.from_str("/client")
  filename = None
  desired_copies = 2

  if args.function != "dump" and args.function != "query":
      filename = Name.from_str(args.filename)
  if args.function == "query":
      query = Name.from_str(args.query)

  if args.function == "insert":
    insertClient = InsertClient(app, client_prefix, repo_prefix)
    await insertClient.insert_file(filename, desired_copies, args.path)
    print("Client finished Insert Command!")
    await asyncio.sleep(20)

  elif args.function == "delete":
    deleteClient = DeleteClient(app, client_prefix, repo_prefix)
    await deleteClient.delete_file(filename)
    print("Client finished Delete Command!")

  elif args.function == "fetch":
    fetchClient = FetchClient(app, client_prefix, repo_prefix)
    await fetchClient.fetch_file(filename, args.path, True)
    print("Client finished Fetch Command!")

  elif args.function == "query":
    queryClient = QueryClient(app, client_prefix, repo_prefix)
    await queryClient.send_query(query, args.sessionid)
    print("Client finished Query Command!")

  elif args.function == "dump":
    dumpClient = DumpClient(app, repo_prefix, args.sessionid)
    try:
        dumpClient.get_view()
    except KeyboardInterrupt:
        pass
    sys.stdout.write('\r')
    sys.stdout.flush()
    print("Client finished Dump Command!")

  else:
    print("Not Implemented Yet / Unknown Command.")

  app.shutdown()

def main() -> None:
    args = parse_cmd_opts()
    app = NDNApp()
    try:
        app.run_forever(after_start=run_client(app, args))
    except FileNotFoundError:
        print('Error: could not connect to NFD.')
        sys.exit()

if __name__ == "__main__":
    sys.exit(main())
