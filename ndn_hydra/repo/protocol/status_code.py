# -------------------------------------------------------------
# NDN Hydra Status Codes
# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Date:    2021-01-25
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/justincpresley/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

from enum import Enum

class StatusCode(Enum):
    # Informational
    STAND_BY = 100          # client needs to do nothing, processing
    FETCHING = 101          # client needs to keep provding file
    # Success
    OK = 200                # client's command went through
    # Redirection
    # Client Error
    BAD_REQUEST = 400       # client sent incorrectly filled command
    UNAUTHORIZED = 401      # client signature is not authorized
    NOT_FOUND = 404         # unable to contact the client
    NO_COMMAND = 405        # status request does not match a command
    # Server Error
    RESOURCE_LIMIT = 500    # storage likely is filled, contact admin
    TRAFFIC_OVERLOAD = 501  # contact at another time and/or contact admin
    NODE_DISCONNECT = 502   # contact node disconnected from hydra, contact admin