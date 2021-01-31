import sys
from insert import InsertClient
from ndn.app import NDNApp
from ndn.encoding import Name
from ndn.security import KeychainDigest

async def run_insert_client(app: NDNApp):
  repo_name = Name.from_str("/testrepo/")
  prefix = Name.from_str("/client/")
  insertClient = InsertClient(app, prefix, repo_name)

  file_name = Name.from_str("~/ndn-distributed-repo/client/example/test.txt")
  total_blocks = 1
  publisher_id = b'\x01\x02\x03\x04'
  fetch_prefix = Name.from_str("/client/") 
  await insertClient.insert_file(file_name, total_blocks, publisher_id, fetch_prefix)
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