import asyncio as aio
import logging
from ..data_storage import DataStorage
from ..global_view import GlobalView
from ndn.app import NDNApp
from ndn.encoding import Name, tlv_var
from ndn_python_repo import Storage


class ReadHandle(object):
    """
    ReadCommandHandle processes ordinary interests, and return corresponding data if exists.
    """
    def __init__(self, app: NDNApp, data_storage: DataStorage, global_view: GlobalView, config: dict):
        """
        :param app: NDNApp.
        :param storage: Storage.
        """
        self.app = app
        self.data_storage = data_storage
        self.global_view = global_view
        self.node_name_comp = "/" + config['node_name']
        self.repo_prefix = config['repo_prefix']

        self.normal_serving_comp = "/main"
        self.personal_serving_comp = "/id"

        self.listen(Name.from_str(config['repo_prefix'] + self.normal_serving_comp))
        self.listen(Name.from_str(config['repo_prefix'] + self.personal_serving_comp  + self.node_name_comp))

    def listen(self, prefix):
        """
        This function needs to be called for prefix of all data stored.
        :param prefix: NonStrictName.
        """
        self.app.route(prefix)(self._on_interest)
        logging.info(f'Read handle: listening to {Name.to_str(prefix)}')

    def unlisten(self, prefix):
        """
        :param name: NonStrictName.
        """
        aio.ensure_future(self.app.unregister(prefix))
        logging.info(f'Read handle: stop listening to {Name.to_str(prefix)}')

    def _on_interest(self, int_name, int_param, _app_param):
        """
        Repo should not respond to any interest with MustBeFresh flag set.
        """
        if int_param.must_be_fresh:
            return
        # get rid of the security part if any on the int_name
        file_name = self._get_file_name_from_interest(Name.to_str(int_name[:-1]))
        best_node_id = self.global_view.best_node_for_file(file_name, self.node_name)
        segment = int(Component.to_str(int_name[-1])[4:])

        if best_node_id == self.node_name:
            #serve content from my storage
            logging.info(f'Read handle: served data {Name.to_str(int_name)}')
            return
        elif best_node_id == None:
            #nack due to lack of avaliablity
            logging.info(f'Read handle: data not found {Name.to_str(int_name)}')
            return
        else:
            #create a link packet with /repo_prefix/id/best_node_id
            logging.info(f'Read handle: redirected {Name.to_str(int_name)}')
            return

    def _get_file_name_from_interest(self, int_name):
        file_name = int_name[len(self.repo_prefix):]
        if file_name[0:len(self.normal_serving_comp)] == self.normal_serving_comp:
            return file_name[len(self.normal_serving_comp):]
        else:
            return file_name[(len(self.personal_serving_comp)+len(self.node_name_comp)):]