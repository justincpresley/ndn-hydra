import sys
from insert import InsertClient
from ndn.app import NDNApp
from ndn.encoding import Name
from ndn.security import KeychainDigest

async def run_insert_client(app: NDNApp):

  repo_prefix = Name.from_str("/pndrepo")
  client_prefix = Name.from_str("/client/")
  insertClient = InsertClient(app, client_prefix, repo_prefix)

  fname = sys.argv[1]
  file_name = Name.from_str(fname)
  desired_copies = 2
  packets = 1
  size = 230
  fetch_prefix = Name.from_str("/client/upload/fo/tes.txt") 
  await insertClient.insert_file(file_name, desired_copies, packets, size, fetch_prefix)
  print("Client finished Insert Command!")
  app.shutdown()


def main():
  app = NDNApp(face=None, keychain=KeychainDigest())
  try:
    app.run_forever(after_start=run_insert_client(app))
  except FileNotFoundError:
      print('Error: could not connect to NFD.')


if __name__ == "__main__":
    sys.exit(main())