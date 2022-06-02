# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Date:    2021-01-25
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/justincpresley/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

from typing import Union
from ndn.encoding import TlvModel, ModelField, NameField, NameField, UintField, BytesField
from ndn_hydra.repo.protocol.base_models import File
from ndn_hydra.repo.protocol.tlv import HydraTlvTypes

class CommandTypes:
    INSERT = 1
    DELETE = 2

class InsertCommand(TlvModel):
    file = ModelField(HydraTlvTypes.FILE, File)
    fetch_path = NameField()

class DeleteCommand(TlvModel):
    file_name = NameField()

class Command(TlvModel):
    type = UintField(HydraTlvTypes.COMMAND_TYPE)
    value = BytesField(HydraTlvTypes.COMMAND)
    def specify(self) -> Union[None, InsertCommand, DeleteCommand]:
        command_type, command_bytes = self.type, bytes(self.value)
        if command_type == CommandTypes.INSERT:
            return InsertCommand.parse(command_bytes)
        elif command_type == CommandTypes.DELETE:
            return RemoveMessage.parse(command_bytes)
        else:
            return None

class FirstContact(TlvModel):
    prefix = NameField()
    curi = NameField()

class Status(TlvModel):
    curi = NameField()
    code = UintField(HydraTlvTypes.STATUS_CODE)
    node = NameField()