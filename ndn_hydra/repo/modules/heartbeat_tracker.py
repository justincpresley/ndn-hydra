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
import logging
from ndn_hydra.repo.modules import *

class HeartbeatTracker:
    class HeartInfo:
        __slots__ = ('past_beat','cycles','alive')
        def __init__(self) -> None:
            self.past_beat, self.cycles, self.alive = 0, 0, False
    def __init__(self, nn:str, gv:GlobalView, lp:int, hr:int, tr:int, btf:int, btr:int):
        self.hearts,self.globalview,self.node_name,self.loop_period,self.heartbeat_rate,self.tracker_rate,self.beats_to_fail,self.beats_to_renew,self.logger = {},gv,nn,lp,hr,tr,btf,btr,logging.getLogger()
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
            if heart.cycles > self.beats_to_renew:
                heart.cycles = 0
                heart.alive = True
                self.globalview.renew_node(node_name)
                self.logger.info(f"[ACT][RENEW]*   nam={node_name}")
    def detect(self):
        for node_name, heart in self.hearts.items():
            time_past = (time.perf_counter()*1000) - heart.past_beat
            if not heart.alive and time_past > self.tracker_rate:
                heart.cycles = 0
            elif time_past > self.tracker_rate:
                heart.cycles = time_past // self.tracker_rate
                if heart.cycles > self.beats_to_fail:
                    heart.cycles = 0
                    heart.alive = False
                    self.globalview.expire_node(node_name)
                    self.logger.info(f"[ACT][EXPIRE]*  nam={node_name}")
    def beat(self):
        try:
            time_past = (time.perf_counter()*1000) - self.hearts[self.node_name].past_beat
            if time_past >= self.heartbeat_rate:
                return True
            else:
                if (time_past+self.loop_period) > self.heartbeat_rate:
                    return True
                else:
                    pass
        except KeyError:
            return True
        return False
    def restart(self, node_name:str):
        self.globalview.expire_node(node_name)
        self.hearts.pop(node_name, None)