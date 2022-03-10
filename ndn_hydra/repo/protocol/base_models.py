# -------------------------------------------------------------
# NDN Hydra Base Tlv Models
# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Date:    2021-01-25
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/justincpresley/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

from ndn.encoding import TlvModel, ModelField, NameField, UintField, RepeatedField, BytesField
from ndn_hydra.repo.protocol.tlv import HydraTlvTypes

class File(TlvModel):
    file_name = NameField()
    packets = UintField(HydraTlvTypes.PACKETS)
    digests = RepeatedField(BytesField(HydraTlvTypes.DIGEST))
    size = UintField(HydraTlvTypes.SIZE)

class FileList(TlvModel):
    list = RepeatedField(ModelField(HydraTlvTypes.FILE, File))

class InsertCommand(TlvModel):
    file = ModelField(HydraTlvTypes.FILE, File)
    fetch_path = NameField()

class DeleteCommand(TlvModel):
    file_name = NameField()

class CommandStatus(TlvModel):
    code = UintField(HydraTlvTypes.STATUS_CODE)

class FirstContact(TlvModel):
    prefix = NameField()
    cmduri = UintField(HydraTlvTypes.CMD_URI)

class NotificationSpecification(TlvModel):
    cmduri = UintField(HydraTlvTypes.CMD_URI)