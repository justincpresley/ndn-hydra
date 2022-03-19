# -------------------------------------------------------------
# NDN Hydra Read Handle
# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Date:    2021-01-25
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/justincpresley/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

import asyncio as aio
import logging
from secrets import choice
from ndn.app import NDNApp
from ndn.encoding import Name, ContentType, Component, parse_data
from ndn.storage import Storage
from ndn_hydra.repo.modules.global_view import GlobalView

class ReadHandle(object):
    """
    ReadHandle processes ordinary interests, and return corresponding data if exists.
    """
    def __init__(self, app: NDNApp, data_storage: Storage, global_view: GlobalView, config: dict):
        """
        :param app: NDNApp.
        :param data_storage: Storage.
        :param global_view: Global View.
        :param config: All config Info.
        """
        self.app = app
        self.data_storage = data_storage
        self.global_view = global_view
        self.node_name = config['node_name']
        self.repo_prefix = config['repo_prefix']

        self.logger = logging.getLogger()

        self.command_comp = "/fetch"
        self.node_comp = "/node"
        # config file needed

        self.listen(Name.from_str(self.repo_prefix + self.command_comp))
        self.listen(Name.from_str(self.repo_prefix + self.node_comp  + self.node_name + self.command_comp))

    def listen(self, prefix):
        """
        This function needs to be called for prefix of all data stored.
        :param prefix: NonStrictName.
        """
        self.app.route(prefix)(self._on_interest)
        self.logger.info(f'Read handle: listening to {Name.to_str(prefix)}')

    def unlisten(self, prefix):
        """
        :param name: NonStrictName.
        """
        aio.ensure_future(self.app.unregister(prefix))
        self.logger.info(f'Read handle: stop listening to {Name.to_str(prefix)}')

    def _on_interest(self, int_name, int_param, _app_param):
        """
        Repo should not respond to any interest with MustBeFresh flag set.
        Repo will:
        - Reply with data of its own
        - Nack if data can not be found within the repo
        - Reply with a redirect to another node
        Assumptions:
        - A node on the on list will have the file in complete form
        """
        if int_param.must_be_fresh:
            return
        # get rid of the security part if any on the int_name
        file_name = self._get_file_name_from_interest(Name.to_str(int_name[:-1]))
        best_id = self._best_id_for_file(file_name)
        segment_comp = "/" + Component.to_str(int_name[-1])

        if best_id == self.node_name:
            if segment_comp == "/seg=0":
                self.logger.info(f'[CMD][FETCH]    serving file')

            # serving my own data
            data_bytes = self.data_storage.get_packet(file_name + segment_comp, int_param.can_be_prefix)
            if data_bytes == None:
                return

            self.logger.debug(f'Read handle: serve data {Name.to_str(int_name)}')
            _, _, content, _ = parse_data(data_bytes)
            # print("serve"+file_name + segment_comp+"   "+Name.to_str(name))
            final_id = Component.from_number(int(self.global_view.get_file(file_name)["packets"])-1, Component.TYPE_SEGMENT)
            self.app.put_data(int_name, content=content, content_type=ContentType.BLOB, final_block_id=final_id)
        elif best_id == None:
            if segment_comp == "/seg=0":
                self.logger.info(f'[CMD][FETCH]    nacked due to no file')

            # nack due to lack of avaliability
            self.app.put_data(int_name, content=None, content_type=ContentType.NACK)
            self.logger.debug(f'Read handle: data not found {Name.to_str(int_name)}')
        else:
            if segment_comp == "/seg=0":
                self.logger.info(f'[CMD][FETCH]    linked to another node')

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

    def _best_id_for_file(self, file_name: str):
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