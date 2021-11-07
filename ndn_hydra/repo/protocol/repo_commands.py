# ----------------------------------------------------------
# NDN Hydra Repo Commands
# ----------------------------------------------------------
# @Project: NDN Hydra
# @Date:    2021-01-25
# @Authors: Please check AUTHORS.rst
# @Source-Code:   https://github.com/UCLA-IRL/ndn-hydra
# @Documentation: https://ndn-hydra.readthedocs.io/
# @Pip-Library:   https://pypi.org/project/ndn-hydra/
# ----------------------------------------------------------

from ndn.encoding import TlvModel, ModelField, NameField, UintField, RepeatedField, BytesField
from ndn_hydra.repo.repo_messages.add import FileTlv

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

class CommandFile(TlvModel):
  file_name = NameField()
  packets = UintField(RepoTypeNumber.PACKETS)
  digests = RepeatedField(BytesField(RepoTypeNumber.DIGEST))
  size = UintField(RepoTypeNumber.SIZE)

class FileList(TlvModel):
    list = RepeatedField(ModelField(RepoTypeNumber.FILE, FileTlv))

class RepoCommand(TlvModel):
  file = ModelField(RepoTypeNumber.FILE, CommandFile)
  sequence_number = UintField(RepoTypeNumber.SEQUENCE_NUMBER)
  fetch_path = ModelField(RepoTypeNumber.FETCH_PATH, FetchPath)