import asyncio
from typing import Optional
from ndn.encoding.ndn_format_0_3 import InterestParam
from ndn.encoding.tlv_type import BinaryStr, FormalName
from client.insert import InsertClient
from client.delete import DeleteClient
import sys

from ndn.app import NDNApp
from ndn.encoding import Name
from ndn.security import KeychainDigest

# def on_interest(name: FormalName, param: InterestParam, _app_param: Optional[BinaryStr]):
  

async def run_insert_client(app: NDNApp):

  repo_prefix = Name.from_str("/pndrepo")
  client_prefix = Name.from_str("/client/")
  insertClient = InsertClient(app, client_prefix, repo_prefix)
  deleteClient = DeleteClient(app, client_prefix, repo_prefix)

  action = sys.argv[1]
  fname = sys.argv[2]
  path = sys.argv[3]
  
  file_name = Name.from_str(fname)
  desired_copies = 2
  packets = 1
  size = 230
  fetch_prefix = Name.from_str("/client/upload/"+fname)

  if action == "i" or action == "insert":
    await insertClient.insert_file(file_name, desired_copies, packets, size, fetch_prefix, path)
    print("Client finished Insert Command!")
  elif action == "d" or action == "delete":
    await deleteClient.delete_file(file_name, desired_copies, packets, size, fetch_prefix)
    print("Client finished Delete Command!")

  await asyncio.sleep(20)
  app.shutdown()


def main():
  app = NDNApp(face=None, keychain=KeychainDigest())

  try:
    app.run_forever(after_start=run_insert_client(app))
  except FileNotFoundError:
      print('Error: could not connect to NFD.')


if __name__ == "__main__":
    sys.exit(main())