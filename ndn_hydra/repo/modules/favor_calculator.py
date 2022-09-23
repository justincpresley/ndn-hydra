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
    LATITUDE = 100
    LONGITUDE = 101
class FavorParameters(TlvModel):
    latitude = BytesField(FavorParameterTypes.LATITUDE)
    longitude = BytesField(FavorParameterTypes.LONGITUDE)
    

class FavorCalculator:
    """
    A class for abstracting favor calculations between two nodes.
    """
    def distance_based_favor(self, latA: float, lonA: float, latB: float, lonB: float) -> float:
        """
        Generate favor for a node based on distance.
        """
        def sphere_dist(latA: float, lonA: float, latB: float, lonB: float) -> float:
            R = 6378.137 # earth radius
            # convert to radians
            latA = latA * (np.pi/180)
            lonA = lonA * (np.pi/180)
            latB = latB * (np.pi/180)
            lonB = lonB * (np.pi/180)
            # calculate distance
            DeltaS = np.arccos(
                np.sin(latA) * np.sin(latB) + \
                np.cos(latA) * np.cos(latB) * np.cos(lonA-lonB)
            )
            distance = R * DeltaS
            return distance

        favor = sphere_dist(latA, lonA, latB, lonB)
        return round(favor, 2)

    