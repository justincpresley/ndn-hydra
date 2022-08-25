# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Date:    2021-01-25
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/justincpresley/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

from __future__ import annotations

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
    @staticmethod
    def from_insert(val:InsertCommand) -> Command:
        cmd = Command()
        cmd.type = CommandTypes.INSERT
        cmd.value = val.encode()
        return cmd
    @staticmethod
    def from_delete(val:DeleteCommand) -> Command:
        cmd = Command()
        cmd.type = CommandTypes.DELETE
        cmd.value = val
        return cmd

class FirstContact(TlvModel):
    prefix = NameField()
    curi = NameField()

class Status(TlvModel):
    curi = NameField()
    code = UintField(HydraTlvTypes.STATUS_CODE)
    node = NameField()