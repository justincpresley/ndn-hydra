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
    
    current_time = time.time()

    # Find files that have not been accessed in the last month
    all_files = global_view.get_files()
    files_to_remove = []
    for file in all_files:
        expire_time = int(file['expiration_time'])
        # If expire_time is 0, file is set to not expire
        if current_time >= expire_time and expire_time != 0:
            files_to_remove.append(file['file_name'])

    # TODO: Implement logger. Check if files were accessed by another node before deleting.

    # Remove files from storage
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
        logger.info(f"[MSG][REMOVE]*\tfil={file_name}")
        svs.publishData(message.encode())

        # Delete from global view
        global_view.delete_file(file_name)
        logger.info(f"GARBAGE COLLECTOR: Removed {file_name} from global view.")

        # TODO: Remove from data storage

    logger.info("GARBAGE COLLECTOR: Finished collecting DB garbage.")

