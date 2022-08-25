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
    OK = 200                # client's request is acceptable
    FULFILLED = 201         # client's command was completed
    # Redirection
    # Client Error
    UNAUTHENTICATED = 400   # client fails to have a valid certificate
    UNAUTHORIZED = 401      # client's command is not authorized
    DUPLICATE = 402         # client's command already exists
    NO_COMMAND = 405        # status request does not match a command
    NOT_FOUND = 404         # unable to contact the client
    BAD_NAME = 405          # filename, curi, or another is name is unacceptable
    BAD_REQUEST = 406       # request was not parsed correctly
    BAD_COMMAND = 407       # command is incorrectly filled out
    BAD_FILE = 408          # provided file is inconsistent or unacceptable
    GHOST_FILE = 409        # file does not exist
    # Server Error
    RESOURCE_LIMIT = 500    # storage likely is filled, contact admin
    TRAFFIC_OVERLOAD = 501  # contact at another time and/or contact admin
    NODE_DISCONNECT = 502   # contact node disconnected from hydra, contact admin
    UNKNOWN_ERROR = 503     # a unknown situation has occured, contact admin