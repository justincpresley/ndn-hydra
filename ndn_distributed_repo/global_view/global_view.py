from threading import Lock
from typing import List
from secrets import choice
from .file_metadata import FileMetadata
from .node_metadata import NodeMetadata
from ndn.svs import StateVector

class GlobalView:
  def __init__(self):
    self.state_vector = StateVector()
    self.file_metadata_dict = {}
    self.node_metadata_dict = {}
    self.lock = Lock()
    #add active insertion id per file
    #store outdated insertion id per file

  def best_node_for_file(self, file_name: str, current_node_name: str):
    if file_name in self.file_metadata_dict:
      if self.file_metadata_dict[file_name].is_file_deleted():
        return None
      on_list = self.file_metadata_dict[file_name].get_on_list()
      if current_node_name in on_list:
        return current_node_name
      else:
        return choice(on_list)
    return None

  def insert_file(self, node_id: str, seqNo: int, insertion_id: str, file_name: str, size: int, on_list: list, backup_list: List, new_favor: int):
    #in case where insertion_id is new, but haven't heard the delete from the previous insertion id, what to do???
    self.lock.acquire()
    try:
      if file_name not in self.file_metadata_dict:
        #First time file is inserted
        file_metadata = FileMetadata(file_name, insertion_id, size, node_id, on_list, backup_list)
        self.file_metadata_dict[file_name] = file_metadata
      else:
        #File has been inserted before
        file_metadata = self.file_metadata_dict[file_name]

        #Check file hasn't been inserted before with this insertion id
        active_insertion_id = file_metadata.get_active_insertion_id()
        inactive_insertion_id_list = file_metadata.get_inactive_insertion_id_list()
        if active_insertion_id == insertion_id or insertion_id in inactive_insertion_id_list:
            raise Exception('Unable to insert {}. File already inserted with insertion id {}'.format(file_name, insertion_id))

        #Check file is deleted
        if not file_metadata.is_file_deleted():
            raise Exception('Unable to insert {}. Have not heard delete for previous insertion id {}'.format(file_name, active_insertion_id))

        #reinsert file
        file_metadata.reinsert_file(insertion_id, node_id, on_list, backup_list)

      if on_list and len(on_list) > 0:
        for node in on_list:
          if node in self.node_metadata_dict:
            self.node_metadata_dict[node].add_file_to_on_list(file_name)
          else:
            self.node_metadata_dict[node] = NodeMetadata(node, new_favor, set([file_name]), set())
      if backup_list and len(backup_list) > 0:
        for node in backup_list:
          if node in self.node_metadata_dict:
            self.node_metadata_dict[node].add_file_to_backup_list(file_name)
          else:
            self.node_metadata_dict[node] = NodeMetadata(node, new_favor, set(), set([file_name]))

      self.state_vector.set(node_id, seqNo)
      self.__update_node_favor(node_id, new_favor)
    finally:
      self.lock.release()

  def delete_file(self, node_id: str, seqNo: int, insertion_id: str, file_name: str, new_favor: int):
    self.lock.acquire()
    try:
      if file_name in self.file_metadata_dict:
        file_metadata = self.file_metadata_dict[file_name]
        active_insertion_id = file_metadata.get_active_insertion_id()
        if active_insertion_id != insertion_id:
          raise Exception('Unable to delete {}. Insertion id {} is not equal to active insertion id {}'.format(file_name, insertion_id, active_insertion_id))

        for node in file_metadata.get_on_list():
          if node in self.node_metadata_dict:
            self.node_metadata_dict[node].remove_file_from_on_list(file_name)
        for node in file_metadata.get_backup_list():
          if node in self.node_metadata_dict:
            self.node_metadata_dict[node].remove_file_from_backup_list(file_name)

        #mark file as deleted
        file_metadata.delete_file(insertion_id)

      else:
        raise Exception('Unable to delete {}. File not known.'.format(file_name))

      self.state_vector.set(node_id, seqNo)
      self.__update_node_favor(node_id, new_favor)
    finally:
      self.lock.release()

  def node_stored_file(self, node_id: str, seqNo: int, insertion_id: str, file_name: str, new_favor: int):
    self.lock.acquire()
    try:
      if file_name in self.file_metadata_dict:
        file_metadata = self.file_metadata_dict[file_name]
        active_insertion_id = file_metadata.get_active_insertion_id()

        if active_insertion_id != insertion_id:
          raise Exception('Unable to complete {} storing file {}. Insertion id {} is not equal to active insertion id {}'.format(node_id, file_name, insertion_id, active_insertion_id))

        if node_id not in self.node_metadata_dict:
          self.node_metadata_dict[node_id] = NodeMetadata(node_id, new_favor, set(), set())

        node_metadata = self.node_metadata_dict[node_id]
        if file_metadata.is_node_in_backup_list(node_id):
          file_metadata.remove_node_from_backup_list(node_id)
          node_metadata.remove_file_from_backup_list(file_name)
        file_metadata.add_node_to_on_list(insertion_id, node_id)
        node_metadata.add_file_to_on_list(file_name)
      else:
        raise Exception('Unable to complete {} storing file {}. File not known'.format(node_id, file_name))

      self.state_vector.set(node_id, seqNo)
      self.__update_node_favor(node_id, new_favor)
    finally:
      self.lock.release()

  def node_claimed_file(self, node_id: str, seqNo: int, insertion_id: str, file_name: str, new_favor: int):
    self.lock.acquire()
    try:
      if file_name in self.file_metadata_dict:
        file_metadata = self.file_metadata_dict[file_name]
        active_insertion_id = file_metadata.get_active_insertion_id()
        if active_insertion_id != insertion_id:
          raise Exception('Unable to complete {} placing new claim on file {}. Insertion id {} is not equal to active insertion id {}'.format(node_id, file_name, insertion_id, active_insertion_id))
        if node_id not in self.node_metadata_dict:
          self.node_metadata_dict[node_id] = NodeMetadata(node_id, new_favor, set(), set())

        node_metadata = self.node_metadata_dict[node_id]
        #check that node wasn't onList and failed and then restarted
        if file_metadata.is_node_in_on_list(node_id):
          file_metadata.remove_node_from_on_list(node_id)
          node_metadata.remove_file_from_on_list(file_name)

        #How to handle if node already on backup_list
        file_metadata.add_node_to_backup_list(insertion_id, node_id)
        node_metadata.add_file_to_backup_list(file_name)
      else:
        raise Exception('Unable to complete {} claimed file {}. File not known'.format(node_name, file_name))

      self.state_vector.set(node_id, seqNo)
      self.__update_node_favor(node_id, new_favor)

    finally:
      self.lock.release()

  def remove_node(self, node_id: str, seqNo: int, node_id_to_remove: str, new_favor: int):
    self.lock.acquire()
    try:
      if node_id_to_remove in self.node_metadata_dict:
        node_metadata = self.node_metadata_dict[node_id_to_remove]
        for file_name in node_metadata.get_backup_list_file_set():
          self.file_metadata_dict[file_name].remove_node_from_backup_list(node_id_to_remove)
        for file_name in node_metadata.get_on_list_file_set():
          self.file_metadata_dict[file_name].remove_node_from_on_list(node_id_to_remove)

        self.node_metadata_dict.pop(node_id_to_remove)

      self.state_vector.set(node_id, seqNo)
      self.__update_node_favor(node_id, new_favor)
    finally:
      self.lock.release()

    def update_node_favor(self, node_id: str, seqNo:int, new_favor: int):
        self.lock.acquire()
        try:
            self.state_vector.set(node_id, seqNo)
            self.__update_node_favor(node_id, new_favor)
        finally:
            self.lock.release()

    def __update_node_favor(self, node_id: str, new_favor: int):
        if node_id in self.node_metadata_dict:
            self.node_metadata_dict[node_id].set_favor(new_favor)
        else:
            self.node_metadata_dict[node_id] = NodeMetadata(node_id, new_favor, set(), set())