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

        self.normal_serving_comp = "/query"
        self.personal_serving_comp = "/sid-query"

        self.listen(Name.from_str(self.repo_prefix + self.normal_serving_comp))
        self.listen(Name.from_str(self.repo_prefix + self.personal_serving_comp  + "/" + self.session_id))

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
            print(f'[cmd][QUERY] query received: sids')
            self.app.put_data(int_name, content=None, freshness_period=3000, content_type=ContentType.NACK)
            return
        elif querytype == "files":
            print(f'[cmd][QUERY] query received: files')
            self.app.put_data(int_name, content=None, freshness_period=3000, content_type=ContentType.NACK)
            return
        elif querytype == "file":
            print(f'[cmd][QUERY] query received: file')
            self.app.put_data(int_name, content=None, freshness_period=3000, content_type=ContentType.NACK)
            return
        elif querytype == "prefix":
            print(f'[cmd][QUERY] query received: prefix')
            self.app.put_data(int_name, content=None, freshness_period=3000, content_type=ContentType.NACK)
            return
        else:
            print(f'[cmd][QUERY] unknown query received')
            self.app.put_data(int_name, content=None, freshness_period=3000, content_type=ContentType.NACK)
            return

    def _get_query_from_interest(self, int_name):
        query = int_name[len(self.repo_prefix):]
        if query[0:len(self.normal_serving_comp)] == self.normal_serving_comp:
            return query[len(self.normal_serving_comp):]
        else:
            return query[(len(self.personal_serving_comp)+len("/" + self.session_id)):]