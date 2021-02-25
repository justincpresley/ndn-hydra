class FileMetadata:
  def __init__(self, file_name: str, size: int, origin: str, on_list = [], backup_list = []):
    self.file_name = file_name
    self.size = size
    self.origin = origin
    self.on_list = on_list
    self.backup_list = backup_list

  def is_node_in_backup_list(node_name):
    #Need to probably change based on types of node_name and backup
    #Need to change if incorporate claim here
    return node_name in self.backup_list
  
  def is_node_in_on_list(node_name):
    #Need to probably change based on types of node_name
    return node_name in self.on_list

  def add_node_to_backup_list(node_name):
    #Need to probably change based on types of node_name and backup
    self.backup_list.append(node_name)

  def add_node_to_on_list(node_name):
    #Need to probably change based on types of node_name
    self.on_list.append(node_name)
  
  def remove_node_from_backup_list(node_name):
    try:
      self.backup_list.remove(node_name)
    except:
      print('Unable to remove {} from backup_list of {}'.format(node_name, self.file_name))
      #Exception? Return False?
      pass

  def remove_node_from_on_list(node_name):
    try:
      self.on_list.remove(node_name)
    except:
      print('Unable to remove {} from on_list of {}'.format(node_name, self.file_name))
      #Exception? Return False?
      pass
  
  
