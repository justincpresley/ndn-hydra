# -------------------------------------------------------------
# NDN Hydra Repo Commands
# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Date:    2021-01-25
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/justincpresley/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

from ndn.encoding import TlvModel, ModelField, NameField, UintField, RepeatedField, BytesField
from ndn_hydra.repo.group_messages.add import FileTlv
from ndn_hydra.repo.protocol.tlv import HydraTlvTypes

class CommandFile(TlvModel):
  file_name = NameField()
  packets = UintField(HydraTlvTypes.PACKETS)
  digests = RepeatedField(BytesField(HydraTlvTypes.DIGEST))
  size = UintField(HydraTlvTypes.SIZE)

class FileList(TlvModel):
    list = RepeatedField(ModelField(HydraTlvTypes.FILE, FileTlv))

class InsertCommand(TlvModel):
    file = ModelField(HydraTlvTypes.FILE, CommandFile)
    sequence_number = UintField(HydraTlvTypes.SEQUENCE_NUMBER)
    fetch_path = NameField()

class DeleteCommand(TlvModel):
    file_name = NameField()
    sequence_number = UintField(HydraTlvTypes.SEQUENCE_NUMBER)