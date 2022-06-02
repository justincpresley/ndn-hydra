# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Date:    2021-01-25
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/justincpresley/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

from typing import Optional
from ndn_hydra.repo.protocol.command_models import Command
from ndn_hydra.repo.protocol.status_code import StatusCode

class CommandBlock:
    __slots__ = ('curi','code','command')
    def __init__(self, curi:str, code:StatusCode, command:Optional[Command]) -> None:
        self.curi, self.code, self.command = curi, code, command

class CommandTable:
    def __init__(self) -> None:
        self.commands = {}
    def set(self, curi:str, code:StatusCode, command:Optional[Command]) -> None:
        self.commands[curi] = CommandBlock(curi, code, command)
    def get(self, curi:str) -> Optional[CommandBlock]:
        try:
            return self.commands[curi]
        except KeyError:
            return None