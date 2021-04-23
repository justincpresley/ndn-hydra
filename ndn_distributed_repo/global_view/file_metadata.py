class FileMetadata:

  def __init__(self, file_name: str, insertion_id: str, size: int, origin_id: str, on_list: list, backup_list: list):
    self.file_name = file_name
    self.size = size
    self.origin_id = origin_id
    self.active_insertion_id = insertion_id
    self.inactive_insertion_id_list = []
    self.on_list = on_list
    self.backup_list = backup_list
    self.isDeleted = False

  def is_file_deleted(self):
    return self.isDeleted

  def is_node_in_backup_list(self, node_id: str):
    try:
      self.__check_file_not_deleted()
      print(self.backup_list)
      return node_id in self.backup_list
    except Exception as e:
      raise e

  def is_node_in_on_list(self, node_id: str):
    try:
      self.__check_file_not_deleted()
      return node_id in self.on_list
    except Exception as e:
      raise e

  def is_active_insertion_id(self, insertion_id: str):
    return insertion_id == self.active_insertion_id

  def is_inactive_insertion_id(self, insertion_id: str):
    return insertion_id in self.inactive_insertion_id_list

  def get_active_insertion_id(self):
    return self.active_insertion_id

  def get_inactive_insertion_id_list(self):
    return self.inactive_insertion_id_list

  def get_backup_list(self):
    try:
      self.__check_file_not_deleted()
      return self.backup_list
    except Exception as e:
      raise e

  def get_on_list(self):
    try:
      self.__check_file_not_deleted()
      return self.on_list
    except Exception as e:
      raise e

  def get_origin(self):
    try:
      self.__check_file_not_deleted()
      return self.origin_id
    except Exception as e:
      raise e

  def add_node_to_backup_list(self, insertion_id: str, node_id: str):
    try:
      self.__check_insertion_id(insertion_id)
      self.__check_file_not_deleted()
      self.backup_list.append(node_id)
    except Exception as e:
      exception_str = 'Unable to add {} to backup list. '.format(node_id)
      raise Exception(exception_str + str(e))

  def add_node_to_on_list(self, insertion_id: str, node_id: str):
    try:
      self.__check_insertion_id(insertion_id)
      self.__check_file_not_deleted()
      self.on_list.append(node_id)
    except Exception as e:
      exception_str = 'Unable to add {} to on list. '.format(node_id)
      raise Exception(exception_str + str(e))

  def remove_node_from_backup_list(self, node_id: str):
    try:
      self.__check_file_not_deleted()
      self.backup_list.remove(node_id)
    except (Exception, ValueError) as e:
      exception_str = 'Unable to remove {} from backup_list of {}. '.format(node_id, self.file_name)
      raise Exception(exception_str + str(e))

  def remove_node_from_on_list(self, node_id: str):
    try:
      self.__check_file_not_deleted()
      self.on_list.remove(node_id)
    except (Exception, ValueError) as e:
      exception_str = 'Unable to remove {} from on_list of {}. '.format(node_id, self.file_name)
      raise Exception(exception_str + str(e))

  def delete_file(self, insertion_id: str):
    try:
      self.__check_insertion_id(insertion_id)
      self.__check_file_not_deleted()
      self.isDeleted = True
    except Exception as e:
      raise Exception('Unable to delete file. ' + str(e))

  def reinsert_file(self, new_insertion_id: str, origin_id: str, on_list = [], backup_list = []):
    try:
      self.__check_file_deleted()
      self.isDeleted = False
      self.inactive_insertion_id_list.append(self.active_insertion_id)
      self.active_insertion_id = new_insertion_id
      self.origin_id = origin_id
      self.on_list = on_list
      self.backup_list = backup_list
    except Exception as e:
      raise Exception('Unable to delete file. ' + str(e))

  def __check_insertion_id(self, insertion_id: str):
    if insertion_id != self.active_insertion_id:
      if is_inactive_insertion_id(insertion_id):
        exception_str += 'Inactive insertion id {}. Active insertion id is {}'\
          .format(insertion_id, self.active_insertion_id)
      else:
        exception_str += 'Unknown insertion id {}. Active insertion id is {}'\
          .format(insertion_id, self.active_insertion_id)
      raise Exception(exception_str)

  def __check_file_not_deleted(self):
    if self.isDeleted:
      raise Exception('File is deleted. Active insertion id is {}'.format(self.active_insertion_id))

  def __check_file_deleted(self):
    if not self.isDeleted:
      raise Exception('File is not deleted. Active insertion id is {}'.format(self.active_insertion_id))