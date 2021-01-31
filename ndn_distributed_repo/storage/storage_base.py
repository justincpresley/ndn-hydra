import asyncio as aio
import logging
from ndn.encoding.tlv_var import parse_tl_num
from ndn.encoding import Name, parse_data, NonStrictName
from ndn.name_tree import NameTrie
import time
from typing import List, Optional


class Storage:
  cache = NameTrie()