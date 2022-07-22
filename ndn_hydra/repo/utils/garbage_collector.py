import time
import logging
from ndn.svs import SVSync
from ndn_hydra.repo.modules.global_view import GlobalView
from ndn_hydra.repo.group_messages.remove import RemoveMessageTlv
from ndn_hydra.repo.group_messages.message import Message, MessageTypes


def collect_db_garbage(global_view: GlobalView, svs: SVSync, config: dict, logger: logging.Logger) -> None:
    """
    Removes files that have not been accessed in the last month from a node's databases.
    """
    logger.info("GARBAGE COLLECTOR: Collecting DB garbage...")

    ONE_MONTH = 60*60*24*30 # seconds in one month
    current_time = time.time()
    all_files = global_view.get_files()

    # Find files that have not been accessed in the last month
    files_to_remove = []
    for file in all_files:
        last_accessed = file['last_accessed']
        if current_time - last_accessed > ONE_MONTH:
            files_to_remove.append(file['file_name'])

    for file_name in files_to_remove:
        # Create RemoveMessage for SVS
        favor = 1.85 # magic number grabbed from another file
        remove_message = RemoveMessageTlv()
        remove_message.node_name = config['node_name'].encode()
        remove_message.favor = str(favor).encode()
        remove_message.file_name = file_name
        # Create Message
        message = Message()
        message.type = MessageTypes.REMOVE
        message.value = remove_message.encode()
        # Send message
        logger.info(f"GARBAGE COLLECTOR: Sending remove message to SVS group for file {file_name}...")
        logger.info(f"[MSG][REMOVE]*\tfil={file_name}")
        svs.publishData(message.encode())
        # Delete from global view
        global_view.delete_file(file_name)
        logger.info(f"GARBAGE COLLECTOR: Removed {file_name} from global view.")

        # TODO: remove from data storage

        logger.info("GARBAGE COLLECTOR: Finished.")

