class NodeToFileMapping:
  def __init__(self, node_name, on_list_file_set=set(), backup_list_file_set=set()):
    self.node_name = node_name
    self.on_list_file_set = on_list_file_set
    self.backup_list_file_set = backup_list_file_set

  def is_file_in_backup_list(file_name: str):
    #May need to change based on types of file_name and backup
    #Need to change if incorporate claim here
    return file_name in self.backup_list_file_set
  
  def is_file_in_on_list(file_name: str):
    #May need to change based on types of file_name
    return file_name in self.on_list_file_set

  def add_file_to_backup_list(file_name: str):
    #Need to change if incorporate claim here
    self.backup_list_file_set.add(file_name)

  def add_file_to_on_list(file_name: str):
    self.on_list_file_set.add(file_name)
  
  def remove_file_from_backup_list(file_name: str):
    #Need to change if incorporate claim here
    try:
      self.backup_list_file_set.remove(file_name)
    except:
      print('Unable to remove {} from backup_list of {}'.format(file_name, self.node_name))
      #Exception? Return False?
      pass
  
  def remove_file_from_on_list(file_name: str):
    try:
      self.on_list_file_set.remove(file_name)
    except:
      print('Unable to remove {} from on_list of {}'.format(file_name, self.node_name))
      #Exception? Return False?
      pass