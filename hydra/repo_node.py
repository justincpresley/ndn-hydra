import logging
from ndn.app import NDNApp
from ndn.encoding import Name

from ndn_python_repo import Storage
from .handle_protocol import *


class RepoNode(object):
    def __init__(self, app: NDNApp, storage: Storage, read_handle: ReadHandle,
                 insert_handle: InsertCommandHandle, delete_handle: DeleteCommandHandle, config: dict):
        """
        An NDN repo instance.
        """
        #self.prefix = Name.from_str(config['repo_config']['repo_name'])
        self.app = app
        self.repo_prefix = Name.from_str(config['repo_prefix'])

        self.storage = storage
        self.insert_handle = insert_handle
        self.read_handle = read_handle
        self.delete_handle = delete_handle

        self.running = True
        #self.register_root = config['repo_config']['register_root']

    async def listen(self):
        """
        Configure pubsub to listen on prefix. The handles share the same pb, so only need to be
        done once.
        This method need to be called to make repo working.
        """
        """
        # Recover registered prefix to enable hot restart
        if not self.register_root:
            self.recover_registered_prefixes()
        """

        # Init PubSub
        self.insert_handle.pb.set_publisher_prefix(self.repo_prefix)
        await self.insert_handle.pb.wait_for_ready()

        await self.insert_handle.listen(self.repo_prefix)
        await self.delete_handle.listen(self.repo_prefix)

    '''
    def recover_registered_prefixes(self):
        prefixes = self.insert_handle.get_registered_prefix_in_storage(self.storage)
        for prefix in prefixes:
            logging.info(f'Existing Prefix Found: {Name.to_str(prefix)}')
            self.read_handle.listen(prefix)
    '''