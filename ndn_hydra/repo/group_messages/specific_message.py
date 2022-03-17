# -------------------------------------------------------------
# NDN Hydra Specific Group Message
# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Date:    2021-01-25
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/justincpresley/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

from ndn_hydra.repo.modules.global_view import GlobalView
import logging

class SpecificMessage:
    def __init__(self, nid:str, seqno:int) -> None:
        self.nid, self.seqno, self.logger = nid, seqno, logging.getLogger()