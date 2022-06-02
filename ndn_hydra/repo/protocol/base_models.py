# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Date:    2021-01-25
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/justincpresley/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

from ndn.encoding import TlvModel, ModelField, NameField, UintField, RepeatedField
from ndn_hydra.repo.protocol.tlv import HydraTlvTypes

class File(TlvModel):
    name = NameField()
    packets = UintField(HydraTlvTypes.PACKETS)
    packet_size = UintField(HydraTlvTypes.PACKET_SIZE)
    size = UintField(HydraTlvTypes.SIZE)

class FileList(TlvModel):
    list = RepeatedField(ModelField(HydraTlvTypes.FILE, File))