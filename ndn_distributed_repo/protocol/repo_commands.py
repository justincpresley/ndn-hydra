"""
    Repo command encoding.
    @Author Daniel Achee
    @Author Caton Zhong
    @Date   2021-1-25
"""

from ndn.encoding import TlvModel, ModelField, NameField, UintField, RepeatedField, BytesField


class RepoTypeNumber:
  REPO_COMMAND = 201
  FILE = 202
  DESIRED_COPIES = 204
  PACKETS = 205
  DIGEST = 206
  SIZE = 207
  FETCH_PATH = 208
  SEQUENCE_NUMBER = 210
  # PUBLISHER_PREFIX = 211
  # NOTIFY_NONCE = 212

class FetchPath(TlvModel):
  prefix = NameField()

class File(TlvModel):
  file_name = NameField()
  desired_copies = UintField(RepoTypeNumber.DESIRED_COPIES)
  packets = UintField(RepoTypeNumber.PACKETS)
  digests = RepeatedField(BytesField(RepoTypeNumber.DIGEST))
  size = UintField(RepoTypeNumber.SIZE)

class RepoCommand(TlvModel):
  file = ModelField(RepoTypeNumber.FILE, File)
  sequence_number = UintField(RepoTypeNumber.SEQUENCE_NUMBER)
  fetch_path = ModelField(RepoTypeNumber.FETCH_PATH, FetchPath)

