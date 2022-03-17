# -------------------------------------------------------------
# NDN Hydra Heartbeat Tracker
# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Date:    2021-01-25
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/justincpresley/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

import time
from ndn_hydra.repo.modules import *

class HeartbeatTracker:
    class HeartInfo:
        __slots__ = ('past_beat','cycles','alive')
        def __init__(self) -> None:
            self.past_beat, self.cycles, self.alive = 0, 0, False
    def __init__(self, node_name:str, globalview:GlobalView, loop_period:int, heartbeat_rate:int, tracker_rate:int):
        self.hearts,self.loop_period,self.heartbeat_rate,self.tracker_rate,self.globalview,self.node_name = {},loop_period,heartbeat_rate,tracker_rate,globalview,node_name
    def reset(self, node_name:str):
        try:
            heart = self.hearts[node_name]
        except KeyError:
            heart = self.HeartInfo()
            self.hearts[node_name] = heart
        heart.past_beat = time.perf_counter() * 1000
        if heart.alive:
            heart.cycles = 0
        else:
            heart.cycles += 1
            if heart.cycles >= 3:
                heart.cycles = 0
                heart.alive = True
                print(f"renewing {node_name}")
                self.globalview.renew_node(node_name)
    def detect(self):
        for node_name, heart in self.hearts.items():
            missed = True if (time.perf_counter()*1000) - heart.past_beat > self.tracker_rate else False
            if not heart.alive and missed:
                heart.cycles = 0
            elif missed:
                heart.cycles += 1
                if heart.cycles >= 3:
                    heart.cycles = 0
                    heart.alive = False
                    self.globalview.expire_node(node_name)
                    print(f"expiring {node_name}")
    def beat(self):
        try:
            heart = self.hearts[self.node_name]
            temp = (time.perf_counter()*1000) - heart.past_beat
            if temp >= self.heartbeat_rate:
                return True
            else:
                if (temp+self.loop_period) > self.heartbeat_rate:
                    return True
                else:
                    pass
        except KeyError:
            return True
        return False