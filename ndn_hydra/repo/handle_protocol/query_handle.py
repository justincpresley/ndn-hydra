# ----------------------------------------------------------
# NDN Hydra Query Handle
# ----------------------------------------------------------
# @Project: NDN Hydra
# @Date:    2021-01-25
# @Author:  Zixuan Zhong
# @Author:  Justin C Presley
# @Author:  Daniel Achee
# @Source-Code: https://github.com/UCLA-IRL/ndn-hydra
# @Pip-Library: https://pypi.org/project/ndn-hydra/
# ----------------------------------------------------------

import asyncio as aio
import logging
from secrets import choice
from ndn.app import NDNApp
from ndn.encoding import Name, tlv_var, ContentType, Component
from ndn_python_repo import Storage
from ndn_hydra.repo.global_view.global_view import GlobalView
from ndn_hydra.repo.protocol.repo_commands import File, FileList

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
        self.session_id = config['session_id']
        self.repo_prefix = config['repo_prefix']

        self.logger = logging.getLogger()

        self.command_comp = "/query"
        self.sid_comp = "/sid"

        self.listen(Name.from_str(self.repo_prefix + self.command_comp))
        self.listen(Name.from_str(self.repo_prefix + self.sid_comp  + "/" + self.session_id + self.command_comp))

    def listen(self, prefix):
        """
        This function needs to be called for prefix of all data stored.
        :param prefix: NonStrictName.
        """
        self.app.route(prefix)(self._on_interest)
        logging.info(f'Query handle: listening to {Name.to_str(prefix)}')

    def unlisten(self, prefix):
        """
        :param name: NonStrictName.
        """
        aio.ensure_future(self.app.unregister(prefix))
        logging.info(f'Query handle: stop listening to {Name.to_str(prefix)}')

    def _on_interest(self, int_name, int_param, _app_param):
        if not int_param.must_be_fresh or not int_param.can_be_prefix:
            return
        query = self._get_query_from_interest(Name.to_str(int_name))
        querytype = Component.to_str(Name.from_str(query)[0])
        if querytype == "sids":
            self.logger.info(f'[cmd][QUERY] query received: sids')
            sessions = self.global_view.get_sessions()
            sidliststr = " ".join([key["id"] for key in sessions])
            self.app.put_data(int_name, content=bytes(sidliststr.encode()), freshness_period=3000, content_type=ContentType.BLOB)
            return
        elif querytype == "files":
            self.logger.info(f'[cmd][QUERY] query received: files')
            insertions = self.global_view.get_insertions()
            filelist = FileList()
            filelist.list = []
            for index in range(len(insertions)):
                file = File()
                file.file_name = insertions[index]["file_name"]
                file.desired_copies = insertions[index]["desired_copies"]
                file.packets = insertions[index]["packets"]
                file.digests = insertions[index]["digests"]
                file.size = insertions[index]["size"]
                filelist.list.append(file)
            self.app.put_data(int_name, content=filelist.encode(), freshness_period=3000, content_type=ContentType.BLOB)
            return
        elif querytype == "file":
            self.logger.info(f'[cmd][QUERY] query received: file')
            insertions = self.global_view.get_insertions()
            filename = query[5:]
            filecontent = None
            for index in range(len(insertions)):
                if Name.to_str(insertions[index]["file_name"]) == filename:
                    file = File()
                    file.file_name = insertions[index]["file_name"]
                    file.desired_copies = insertions[index]["desired_copies"]
                    file.packets = insertions[index]["packets"]
                    file.digests = insertions[index]["digests"]
                    file.size = insertions[index]["size"]
                    filecontent = file.encode()
                    break
            self.app.put_data(int_name, content=filecontent, freshness_period=3000, content_type=ContentType.BLOB)
            return
        elif querytype == "prefix":
            self.logger.info(f'[cmd][QUERY] query received: prefix')
            insertions = self.global_view.get_insertions()
            prefix = query[7:]
            filelist = FileList()
            filelist.list = []
            for index in range(len(insertions)):
                if Name.is_prefix(Name.from_str(prefix), insertions[index]["file_name"]):
                    file = File()
                    file.file_name = insertions[index]["file_name"]
                    file.desired_copies = insertions[index]["desired_copies"]
                    file.packets = insertions[index]["packets"]
                    file.digests = insertions[index]["digests"]
                    file.size = insertions[index]["size"]
                    filelist.list.append(file)
            self.app.put_data(int_name, content=filelist.encode(), freshness_period=3000, content_type=ContentType.BLOB)
            return
        else:
            self.logger.info(f'[cmd][QUERY] unknown query received')
            self.app.put_data(int_name, content=None, freshness_period=3000, content_type=ContentType.NACK)
            return

    def _get_query_from_interest(self, int_name):
        query = int_name[len(self.repo_prefix):]
        if query[0:len(self.sid_comp)] == self.sid_comp:
            return query[(len(self.sid_comp)+len("/" + self.session_id)+len(self.command_comp)):]
        else:
            return query[(len(self.command_comp)):]