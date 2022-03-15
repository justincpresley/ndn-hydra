# -------------------------------------------------------------
# NDN Hydra Delete Command Handle
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
import time
from ndn.app import NDNApp
from ndn.encoding import Name, NonStrictName, Component
from ndn.storage import Storage
from ndn_hydra.repo.protocol.base_models import DeleteCommand
from ndn_hydra.repo.utils.pubsub import PubSub
from ndn_hydra.repo.group_messages.remove import RemoveMessageTlv
from ndn_hydra.repo.group_messages.message import Message, MessageTypes
from ndn_hydra.repo.main.main_loop import MainLoop
from ndn_hydra.repo.handles.protocol_handle_base import ProtocolHandle
from ndn_hydra.repo.modules.global_view import GlobalView

class DeleteCommandHandle(ProtocolHandle):
    """
    DeleteCommandHandle processes delete command handles, and deletes corresponding data stored
    in the database.
    TODO: Add validator
    """
    def __init__(self, app: NDNApp, data_storage: Storage, pb: PubSub, config: dict,
                main_loop: MainLoop, global_view: GlobalView):
        """
        :param app: NDNApp.
        :param data_storage: Storage.
        :param pb: PubSub.
        :param config: All config Info.
        :param main_loop: SVS interface, Group Messages.
        :param global_view: Global View.
        """
        super(DeleteCommandHandle, self).__init__(app, data_storage, pb, config)
        self.prefix = None
        self.main_loop = main_loop
        self.global_view = global_view
        self.repo_prefix = config['repo_prefix']
        #self.register_root = config['repo_config']['register_root']

    async def listen(self, prefix: NonStrictName):
        """
        Register routes for command interests.
        This function needs to be called explicitly after initialization.
        :param name: NonStrictName. The name prefix to listen on.
        """
        self.prefix = prefix
        self.logger.info(f'Insert handle: subscribing to {Name.to_str(self.prefix) + "/delete"}')
        self.pb.subscribe(self.prefix + ['delete'], self._on_delete_msg)
        # start to announce process status
        # await self._schedule_announce_process_status(period=3)

    def _on_delete_msg(self, msg):

        try:
            cmd = DeleteCommand.parse(msg)
            # if cmd.name == None:
            #     raise DecodeError()
        except (DecodeError, IndexError) as exc:
            logging.warning('Parameter interest decoding failed')
            return
        aio.ensure_future(self._process_delete(cmd))

    async def _process_delete(self, cmd: DeleteCommand):
        """
        Process delete command.
        """

        file_name = cmd.file_name

        self.logger.info("[cmd][DELETE] file {}".format(Name.to_str(file_name)))

        file = self.global_view.get_file_by_name(Name.to_str(file_name))
        if file == None:
            self.logger.warning("file does not exist")
            return

        insertion_id = file['id']
        # add tlv
        expire_at = int(time.time()+(self.config['period']*2))
        favor = 1.85
        remove_message = RemoveMessageTlv()
        remove_message.node_name = self.config['node_name'].encode()
        remove_message.expire_at = expire_at
        remove_message.favor = str(favor).encode()
        remove_message.insertion_id = insertion_id.encode()
        # add msg
        message = Message()
        message.type = MessageTypes.REMOVE
        message.value = remove_message.encode()
        self.global_view.delete_file(insertion_id)
        self.main_loop.svs.publishData(message.encode())
        val = "[MSG][REMOVE]* iid={iid}".format(
            iid=insertion_id
        )
        self.logger.info(val)