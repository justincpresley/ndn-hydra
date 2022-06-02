# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Date:    2021-01-25
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/justincpresley/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

import logging
from secrets import choice
from typing import Optional
from ndn.app import NDNApp
from ndn.encoding import Name, FormalName, InterestParam, ContentType, BinaryStr, SignaturePtrs, Component, parse_data
from ndn.storage import Storage
from ndn_hydra.repo.handles.handle import Handle
from ndn_hydra.repo.modules.global_view import GlobalView

class ReadHandle(Handle):
    def __init__(self, app:NDNApp, config:dict, global_view:GlobalView, data_storage:Storage) -> None:
        self.app = app
        self.data_storage = data_storage
        self.global_view = global_view
        self.node_name = config['node_name']
        self.repo_prefix = config['repo_prefix']
        self.command_comp = "/fetch"
        self.node_comp = "/node"
        self.read_prefix = Name.from_str(self.repo_prefix + self.command_comp)
        self.node_read_prefix = Name.from_str(self.repo_prefix + self.node_comp  + self.node_name + self.command_comp)
    async def listen(self) -> None:
        await self.app.register(self.read_prefix, self.onReadInterest, need_sig_ptrs=True)
        logging.info(f'ReadHandle: listening {Name.to_str(self.read_prefix)}')
        await self.app.register(self.node_read_prefix, self.onReadInterest, need_sig_ptrs=True)
        logging.info(f'ReadHandle: listening {Name.to_str(self.node_read_prefix)}')
    def onReadInterest(self, int_name:FormalName, int_param:InterestParam, app_param:Optional[BinaryStr], sig_ptrs:SignaturePtrs) -> None:
        if int_param.must_be_fresh:
            return

        # get rid of the security part if any on the int_name
        file_name = self._get_file_name_from_interest(Name.to_str(int_name[:-1]))
        best_id = self._best_id_for_file(file_name)
        segment_comp = "/" + Component.to_str(int_name[-1])

        if best_id == self.node_name:
            if segment_comp == "/seg=0":
                logging.info(f'[CMD][FETCH]    serving file')

            # serving my own data
            data_bytes = self.data_storage.get_packet(file_name + segment_comp, int_param.can_be_prefix)
            if data_bytes == None:
                return

            logging.debug(f'Read handle: serve data {Name.to_str(int_name)}')
            _, _, content, _ = parse_data(data_bytes)
            # print("serve"+file_name + segment_comp+"   "+Name.to_str(name))
            final_id = Component.from_number(int(self.global_view.get_file(file_name)["packets"])-1, Component.TYPE_SEGMENT)
            self.app.put_data(int_name, content=content, content_type=ContentType.BLOB, final_block_id=final_id)
        elif best_id == None:
            if segment_comp == "/seg=0":
                logging.info(f'[CMD][FETCH]    nacked due to no file')

            # nack due to lack of avaliability
            self.app.put_data(int_name, content=None, content_type=ContentType.NACK)
            logging.debug(f'Read handle: data not found {Name.to_str(int_name)}')
        else:
            if segment_comp == "/seg=0":
                logging.info(f'[CMD][FETCH]    linked to another node')

            # create a link to a node who has the content
            new_name = self.repo_prefix + self.node_comp + best_id + self.command_comp + file_name
            link_content = bytes(new_name.encode())
            final_id = Component.from_number(int(self.global_view.get_file(file_name)["packets"])-1, Component.TYPE_SEGMENT)
            self.app.put_data(int_name, content=link_content, content_type=ContentType.LINK, final_block_id=final_id)
    def _get_file_name_from_interest(self, int_name):
        file_name = int_name[len(self.repo_prefix):]
        if file_name[0:len(self.node_comp)] == self.node_comp:
            return file_name[(len(self.node_comp)+len(self.node_name)+len(self.command_comp)):]
        else:
            return file_name[(len(self.command_comp)):]
    def _best_id_for_file(self, file_name:str):
        file_info = self.global_view.get_file(file_name)
        active_nodes = set( [x['node_name'] for x in self.global_view.get_nodes()] )
        if file_name == None:
            return None
        on_list = file_info["stores"]
        if file_info["is_deleted"] == True or not on_list:
            return None
        if self.node_name in on_list:
            return self.node_name
        else:
            on_list = [x for x in on_list if x in active_nodes]
            return choice(on_list)