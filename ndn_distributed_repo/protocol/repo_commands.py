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
  TOTAL_BLOCKS = 203
  PUBLISHER_ID = 204
  FETCH_PREFIX = 205
  PUBLISHER_PREFIX = 210
  NOTIFY_NONCE = 211

class FetchPrefix(TlvModel):
  name = NameField()

class File(TlvModel):
  name = NameField()
  totalBlocks = UintField(RepoTypeNumber.TOTAL_BLOCKS)
  publisherId = BytesField(RepoTypeNumber.PUBLISHER_ID)

class RepoCommand(TlvModel):
  file = ModelField(RepoTypeNumber.FILE, File)
  fetchPrefix = ModelField(RepoTypeNumber.FETCH_PREFIX, FetchPrefix)

