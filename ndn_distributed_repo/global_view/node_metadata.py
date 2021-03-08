class NodeMetadata:
  def __init__(self, node_id: str, favor: int, on_list_file_set: set, backup_list_file_set: set):
    self.node_id = node_id
    self.favor = favor
    self.on_list_file_set = on_list_file_set
    self.backup_list_file_set = backup_list_file_set

  def is_file_in_backup_list(self, file_name: str):
    return file_name in self.backup_list_file_set
  
  def is_file_in_on_list(self, file_name: str):
    return file_name in self.on_list_file_set

  def get_backup_list_file_set(self):
    return self.backup_list_file_set

  def get_on_list_file_set(self):
    return self.on_list_file_set

  def add_file_to_backup_list(self, file_name: str):
    self.backup_list_file_set.add(file_name)

  def add_file_to_on_list(self, file_name: str):
    self.on_list_file_set.add(file_name)
  
  def remove_file_from_backup_list(self, file_name: str):
    try:
      self.backup_list_file_set.remove(file_name)
    except:
      raise Exception('Unable to remove {} from backup_list of {}'.format(file_name, self.node_id))
  
  def remove_file_from_on_list(self, file_name: str):
    try:
      self.on_list_file_set.remove(file_name)
    except:
      raise Exception('Unable to remove {} from on_list of {}'.format(file_name, self.node_id))

  def get_favor(self):
    return self.favor

  def set_favor(self, favor: int):
    self.favor = favor
