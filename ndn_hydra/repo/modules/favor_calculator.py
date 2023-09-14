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
from os import shutil


class FavorParameterTypes:
#    RTT = 100
    NETWORK_COST_PER_GB = 0.01
    STOARGE_COST_PER_GB = 0.014
    NUM_USERS = 100
    BANDWIDTH = 25000 #Mbps
    NETWORK_COST = COST_PER_GB * (BANDWIDTH/(1000*8)) #0.01 USD/GB  
    RW_SPEED = 6.25
    TOTAL_STORAGE, USED_STORAGE, REMAINING_STORAGE = shutil.disk_usage(__file__)
    STORAGE_COST = REMAINING_STORAGE * STORAGE_COST_PER_GB

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
        #for param, val in favor_parameters.asdict().items():
            # print(param, ':', val)
        #    favor += int(val)
        # print('favor:', favor)
        favor = .3*REMAINING_STORAGE + .3*BANDWIDTH + .4*RW_SPEED + 0.0*NUM_USERS + 0.0*NETWORK_COST + 0.0*STORAGE_COST
        return favor


    
