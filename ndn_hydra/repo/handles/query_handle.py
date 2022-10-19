# -------------------------------------------------------------
# NDN Hydra Query Handle
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
from ndn.app import NDNApp
from ndn.encoding import Name, ContentType, Component
from ndn.storage import Storage
from ndn_hydra.repo.modules.global_view import GlobalView
from ndn_hydra.repo.protocol.base_models import FileList, File

class QueryHandle(object):
    """
    QueryHandle processes query interests, and return informational data.
    """
    def __init__(self, app: NDNApp, global_view: GlobalView, config: dict):
        """
        :param app: NDNApp.
        :param global_view: Global View.
        :param config: All config Info.
        """
        self.app = app
        self.global_view = global_view
        self.node_name = config['node_name']
        self.repo_prefix = config['repo_prefix']

        self.logger = logging.getLogger()

        self.command_comp = "/query"
        self.node_comp = "/node"

        self.listen(Name.from_str(self.repo_prefix + self.command_comp))
        self.listen(Name.from_str(self.repo_prefix + self.node_comp  + self.node_name + self.command_comp))

    def listen(self, prefix):
        """
        This function needs to be called for prefix of all data stored.
        :param prefix: NonStrictName.
        """
        self.app.route(prefix)(self._on_interest)
        self.logger.info(f'Query handle: listening to {Name.to_str(prefix)}')

    def unlisten(self, prefix):
        """
        :param name: NonStrictName.
        """
        aio.ensure_future(self.app.unregister(prefix))
        self.logger.info(f'Query handle: stop listening to {Name.to_str(prefix)}')

    def _on_interest(self, int_name, int_param, _app_param):
        if not int_param.must_be_fresh or not int_param.can_be_prefix:
            return 
        if not _app_param:
            self.logger.info('Query handle: No querytype')
            return

        query = Name.from_bytes(_app_param)
        querytype = Component.to_str(query[0])
        query = Name.to_str(query)

        if querytype == "nodes":
            self.logger.info(f'[CMD][QUERY]    query received: nodes')
            nodes = self.global_view.get_nodes()
            nodenamestrlist = " ".join([key["node_name"] for key in nodes])
            self.app.put_data(int_name, content=bytes(nodenamestrlist.encode()), freshness_period=3000, content_type=ContentType.BLOB)
            return
        elif querytype == "exnodes":
            self.logger.info(f'[CMD][QUERY]    query received: exnodes')
            nodes = self.global_view.get_nodes(True)
            nodenamestrlist = " ".join([key["node_name"] for key in nodes])
            self.app.put_data(int_name, content=bytes(nodenamestrlist.encode()), freshness_period=3000, content_type=ContentType.BLOB)
            return
        elif querytype == "files":
            self.logger.info(f'[CMD][QUERY]    query received: files')
            files = self.global_view.get_files()
            filelist = FileList()
            filelist.list = []
            for index in range(len(files)):
                file = File()
                file.file_name = files[index]["file_name"]
                file.packets = files[index]["packets"]
                file.packet_size = files[index]["packet_size"]
                file.size = files[index]["size"]
                filelist.list.append(file)
            self.app.put_data(int_name, content=filelist.encode(), freshness_period=3000, content_type=ContentType.BLOB)
            return
        elif querytype == "file":
            self.logger.info(f'[CMD][QUERY]    query received: file')
            files = self.global_view.get_files()
            filename = query[5:]
            filecontent = None
            for index in range(len(files)):
                if Name.to_str(files[index]["file_name"]) == filename:
                    file = File()
                    file.file_name = files[index]["file_name"]
                    file.packets = files[index]["packets"]
                    file.packet_size = files[index]["packet_size"]
                    file.size = files[index]["size"]
                    filecontent = file.encode()
                    break
            self.app.put_data(int_name, content=filecontent, freshness_period=3000, content_type=ContentType.BLOB)
            return
        elif querytype == "prefix":
            self.logger.info(f'[CMD][QUERY]    query received: prefix')
            files = self.global_view.get_files()
            prefix = query[7:]
            filelist = FileList()
            filelist.list = []
            for index in range(len(files)):
                if Name.is_prefix(Name.from_str(prefix), files[index]["file_name"]):
                    file = File()
                    file.file_name = files[index]["file_name"]
                    file.packets = files[index]["packets"]
                    file.packet_size = files[index]["packet_size"]
                    file.size = files[index]["size"]
                    filelist.list.append(file)
            self.app.put_data(int_name, content=filelist.encode(), freshness_period=3000, content_type=ContentType.BLOB)
            return
        else:
            self.logger.info(f'[CMD][QUERY]    unknown query received')
            self.app.put_data(int_name, content=None, freshness_period=3000, content_type=ContentType.NACK)
            return
