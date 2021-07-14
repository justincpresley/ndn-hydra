# ----------------------------------------------------------
# NDN Hydra Delete Command Handle
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
import time
import random
from ndn.app import NDNApp
from ndn.encoding import Name, NonStrictName, Component
from ndn_python_repo import Storage
from ndn_hydra.repo.protocol.repo_commands import RepoCommand
from ndn_hydra.repo.utils.pubsub import PubSub
from ndn_hydra.repo.repo_messages.remove import RemoveMessageBodyTlv
from ndn_hydra.repo.repo_messages.message import MessageTlv, MessageTypes
from ndn_hydra.repo.main.main_loop import MainLoop
from ndn_hydra.repo.handle_protocol.protocol_handle_base import ProtocolHandle
from ndn_hydra.repo.global_view.global_view import GlobalView

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
        #self.register_root = config['repo_config']['register_root']

    async def listen(self, prefix: NonStrictName):
        """
        Register routes for command interests.
        This function needs to be called explicitly after initialization.
        :param name: NonStrictName. The name prefix to listen on.
        """
        self.prefix = prefix

        # subscribe to delete messages
        self.pb.subscribe(self.prefix + ['delete'], self._on_delete_msg)

        # start to announce process status
        # await self._schedule_announce_process_status(period=3)

    def _on_delete_msg(self, msg):

        try:
            cmd = RepoCommand.parse(msg)
            # if cmd.name == None:
            #     raise DecodeError()
        except (DecodeError, IndexError) as exc:
            logging.warning('Parameter interest decoding failed')
            return
        aio.ensure_future(self._process_delete(cmd))

    async def _process_delete(self, cmd: RepoCommand):
        """
        Process delete command.
        """

        file_name = cmd.file.file_name
        sequence_number = cmd.sequence_number

        self.logger.info("[cmd][DELETE] file {}".format(Name.to_str(file_name)))

        insertion = self.global_view.get_insertion_by_file_name(Name.to_str(file_name))
        if insertion == None:
            self.logger.warning("file does not exist")
            return

        insertion_id = insertion['id']
        # add tlv
        expire_at = int(time.time()+(self.config['period']*2))
        favor = 1.85
        remove_message_body = RemoveMessageBodyTlv()
        remove_message_body.session_id = self.config['session_id'].encode()
        remove_message_body.node_name = self.config['node_name'].encode()
        remove_message_body.expire_at = expire_at
        remove_message_body.favor = str(favor).encode()
        remove_message_body.insertion_id = insertion_id.encode()
        # add msg
        remove_message = MessageTlv()
        remove_message.header = MessageTypes.REMOVE
        remove_message.body = remove_message_body.encode()
        self.global_view.delete_insertion(insertion_id)
        self.main_loop.svs.publishData(remove_message.encode())
        val = "[MSG][REMOVE]* iid={iid}".format(
            iid=insertion_id
        )
        self.logger.info(val)






