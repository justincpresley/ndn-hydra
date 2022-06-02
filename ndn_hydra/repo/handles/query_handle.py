# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Date:    2021-01-25
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/justincpresley/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

import logging
from typing import Optional
from ndn.app import NDNApp
from ndn.encoding import Name, ContentType, FormalName, InterestParam, BinaryStr, SignaturePtrs, Component
from ndn_hydra.repo.handles.handle import Handle
from ndn_hydra.repo.modules.global_view import GlobalView
from ndn_hydra.repo.protocol.base_models import FileList, File

class QueryHandle(Handle):
    def __init__(self, app:NDNApp, config:dict, global_view:GlobalView) -> None:
        self.app = app
        self.global_view = global_view
        self.node_name = config['node_name']
        self.repo_prefix = config['repo_prefix']
        self.command_comp = "/query"
        self.node_comp = "/node"
        self.query_prefix = Name.from_str(self.repo_prefix + self.command_comp)
        self.node_query_prefix = Name.from_str(self.repo_prefix + self.node_comp  + self.node_name + self.command_comp)
    async def listen(self) -> None:
        await self.app.register(self.query_prefix, self.onQueryInterest, need_sig_ptrs=True)
        logging.info(f'QueryHandle: listening {Name.to_str(self.query_prefix)}')
        await self.app.register(self.node_query_prefix, self.onQueryInterest, need_sig_ptrs=True)
        logging.info(f'QueryHandle: listening {Name.to_str(self.node_query_prefix)}')
    def onQueryInterest(self, int_name:FormalName, int_param:InterestParam, app_param:Optional[BinaryStr], sig_ptrs:SignaturePtrs) -> None:
        if not int_param.must_be_fresh or not int_param.can_be_prefix:
            return
        query = self._get_query_from_interest(Name.to_str(int_name))
        querytype = Component.to_str(Name.from_str(query)[0])
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

    def _get_query_from_interest(self, int_name):
        query = int_name[len(self.repo_prefix):]
        if query[0:len(self.node_comp)] == self.node_comp:
            return query[(len(self.node_comp)+len(self.node_name)+len(self.command_comp)):]
        else:
            return query[(len(self.command_comp)):]