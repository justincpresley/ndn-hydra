# -------------------------------------------------------------
# NDN Hydra Favor Calculator
# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Date:    2021-01-25
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/justincpresley/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

import logging
import numpy as np
from ndn.encoding import *

class FavorParameterTypes:
    RTT = 100
    NUM_USERS = 101
    BANDWIDTH = 102
    NETWORK_COST = 103
    STORAGE_COST = 104
    REMAINING_STORAGE = 105

class FavorParameters(TlvModel):
    rtt = BytesField(FavorParameterTypes.RTT)
    num_users = BytesField(FavorParameterTypes.NUM_USERS)
    bandwidth = BytesField(FavorParameterTypes.BANDWIDTH)
    network_cost = BytesField(FavorParameterTypes.NETWORK_COST)
    storage_cost = BytesField(FavorParameterTypes.STORAGE_COST)
    remaining_storage = BytesField(FavorParameterTypes.REMAINING_STORAGE)
    

class FavorCalculator:
    """
    A class for abstracting favor calculations between two nodes.
    """
    def calculate_favor(self, favor_parameters: FavorParameters) -> float:
        favor = 0
        for param, val in favor_parameters.asdict().items():
            # print(param, ':', val)
            favor += int(val)
        # print('favor:', favor)
        return favor


    