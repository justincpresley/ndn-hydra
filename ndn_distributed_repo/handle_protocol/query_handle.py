import asyncio as aio
import logging
from secrets import choice
from ..global_view import GlobalView
from ndn.app import NDNApp
from ndn.encoding import Name, tlv_var, ContentType, Component
from ndn_python_repo import Storage

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
        return