from typing import List
from .file_metadata import FileMetadata
from .node_to_file_mapping import NodeToFileMapping
from svs.version_vector import VersionVector

class GlobalView:

  def __init__(self, file_metadata_dict={}, node_to_file_mapping_dict={}):
    self.state_vector = VersionVector()
    self.file_metadata_dict = file_metadata_dict
    self.node_to_file_mapping_dict = node_to_file_mapping_dict

  def insert_file(self, node_id: str, seqNo: int, file_name: str, size, origin: str, on_list: List, backup_list: List):
    if file_name not in self.file_metadata_dict:
      metadata = FileMetadata(file_name, size, origin, on_list, backup_list)
      self.state_vector.set(node_id, seqNo)
      self.file_metadata_dict[file_name] = metadata
      if on_list and len(on_list) > 0:
        for node in on_list:
          if node in node_to_file_mapping_dict:
            self.node_to_file_mapping_dict[node].add_file_to_on_list(file_name)
          else:
            self.node_to_file_mapping_dict[node] = NodeToFileMapping(node, set([file_name]))
      if backup_list and len(backup_list) > 0:
        for node in backup_list:
          if node in node_to_file_mapping_dict:
            self.node_to_file_mapping_dict[node].add_file_to_backup_list(file_name)
          else:
            self.node_to_file_mapping_dict[node] = NodeToFileMapping(node, set(), set([file_name]))
    else:
      print('Unable to insert {}. File already in inserted.'.format(file_name))
      #Error? Need to Buffer Packet?
      #Perhaps need to delete current on_list and backup_list??
      #should I set state vector in else?
      return False
    return True

  def delete_file(self, node_id: str, seqNo: int, file_name: str):
    if file_name in file_metadata_dict:
      self.state_vector.set(node_id, seqNo)
      metadata = file_metadata_dict[file_name]
      node_to_file_mapping = self.node_to_file_mapping_dict[node]
      for node in metadata.on_list:
        node_to_file_mapping.remove_file_from_on_list(file_name)
      for node in metadata.backup_list:
        node_to_file_mapping.remove_file_from_backup_list(file_name)
      self.file_metadata_dict.pop(file_name)
    else:
      print('Unable to delete {}. File not known.'.format(file_name))
      #Error? Need to Buffer Packet?
      #should I set state vector in else?
      return False
    return True 

  def node_stored_file(self, node_id: str, seqNo: int, file_name, node_name):
    #what happens if file_name not in repo yet?
    if file_name in file_metadata_dict:
      self.state_vector.set(node_id, seqNo)
      metadata = self.file_metadata_dict[file_name]
      node_to_file_mapping = self.node_to_file_mapping_dict[node_name]
      if metadata.is_node_in_backup_list(node_name):
        metadata.remove_node_from_backup_list(node_name)
        node_to_file_mapping.remove_file_from_backup_list(file_name)
      metadata.add_node_to_on_list(node_name)
      node_to_file_mapping.add_file_to_on_list(file_name)
    else:
      print('Unable to complete {} storing file {}. File not known'.format(node_name, file_name))
      #Exception? NeedtoBufferPacket?
      #should I set state vector in else?
      return False
    return True 

  def new_backup_node(self, node_id: str, seqNo: int, file_name, node_name):
    #when replaying messages, we need claim in order to order backups
    #since we won't be able to do so based on message order since no total ordering
    metadata = self.file_metadata_dict[file_name]
    node_to_file_mapping = self.node_to_file_mapping_dict[node_name]
    if metadata:
      self.state_vector.set(node_id, seqNo)
      #check that node wasn't onList and failed and then restarted
      if metadata.is_node_in_on_list(node_name):
        metadata.remove_node_from_on_list(node_name)
        node_to_file_mapping.remove_file_from_on_list(file_name)
      metadata.add_node_to_backup_list(node_name)
      node_to_file_mapping.add_file_to_backup_list(file_name)
    else:
      print('Unable to complete {} becoming new backup node for file {}. File not known'.format(node_name, file_name))
      #Exception? NeedtoBufferPacket?
      return False
    return True 

  def remove_node(self, node_id: str, seqNo: int, node_name):
    self.state_vector.set(node_id, seqNo)
    #TODO