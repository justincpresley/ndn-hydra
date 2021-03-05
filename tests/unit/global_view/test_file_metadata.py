import pytest
from ndn_distributed_repo.global_view.file_metadata import FileMetadata

class TestFileMetadata:
  def test_file_deletion(self):
    metadata = FileMetadata('testFile', '1', 100, 'testOrigin', [], [])
    assert not metadata.is_file_deleted()

    #add node to backup list and on list for later tests
    #want to ensure exception thrown because file deleted
    #not because of valueError because because trying
    #to remove item not in list
    metadata.add_node_to_backup_list('1', 'testNode')
    metadata.add_node_to_on_list('1', 'testNode')

    #check reinsert_file checks that file must be deleted
    with pytest.raises(Exception):
      metadata.reinsert_file('2')

    #delete file
    metadata.delete_file('1')

    assert metadata.is_file_deleted()

    #check functions raise error once file deleted
    with pytest.raises(Exception):
      metadata.is_node_in_backup_list('testNode')

    with pytest.raises(Exception):
      metadata.is_node_in_on_list('testNode')

    with pytest.raises(Exception):
      metadata.get_origin()

    with pytest.raises(Exception):
      metadata.get_backup_list()

    with pytest.raises(Exception):
      metadata.get_on_list()

    with pytest.raises(Exception):
      metadata.add_node_to_backup_list('1', 'testNode')

    with pytest.raises(Exception):
      metadata.add_node_to_on_list('1', 'testNode')

    with pytest.raises(Exception):
      metadata.remove_node_from_backup_list('1', 'testNode')

    with pytest.raises(Exception):
      metadata.remove_node_from_on_list('1', 'testNode')

    with pytest.raises(Exception):
      metadata.delete_file('1')

  def test_insertion_id(self):    
    metadata = FileMetadata('testFile', '1', 100, 'testOrigin', [], [])

    assert '1' == metadata.get_active_insertion_id()
    assert metadata.is_active_insertion_id('1')

    assert len(metadata.get_inactive_insertion_id_list()) == 0

    assert not metadata.is_inactive_insertion_id('2')

  def test_check_insertion_id(self):
    metadata = FileMetadata('testFile', '1', 100, 'testOrigin', [], [])

    with pytest.raises(Exception):
      metadata.add_node_to_backup_list('2', 'testNode')

    with pytest.raises(Exception):
      metadata.add_node_to_on_list('2', 'testNode')

    with pytest.raises(Exception):
      metadata.add_node_to_on_list('2', 'testNode')
  
    with pytest.raises(Exception):
      metadata.delete_file('2')
    
  def test_reinsert_file(self):
    metadata = FileMetadata('testFile', '1', 100, 'testOrigin', [], [])

    #check reinsert_file checks that file must be deleted
    with pytest.raises(Exception):
      metadata.reinsert_file('2')

    metadata.add_node_to_backup_list('1', 'testNode')
    metadata.add_node_to_on_list('1', 'testNode2')
    metadata.delete_file('1')

    metadata.reinsert_file('2', 'testOrigin2')

    assert not metadata.is_file_deleted()
    inactive_insertion_id_list = metadata.get_inactive_insertion_id_list()
    assert len(inactive_insertion_id_list) == 1
    assert inactive_insertion_id_list[0] == '1'
    assert metadata.get_active_insertion_id() == '2'
    assert metadata.get_origin() == 'testOrigin2'
    assert not metadata.is_node_in_backup_list('testNode')
    assert not metadata.is_node_in_on_list('testNode2')

    metadata.delete_file('2')
    metadata.reinsert_file('3', 'testOrigin3')
    inactive_insertion_id_list = metadata.get_inactive_insertion_id_list()
    assert len(inactive_insertion_id_list) == 2
    assert '1' in inactive_insertion_id_list
    assert '2' in inactive_insertion_id_list
    assert not metadata.is_node_in_backup_list('testNode')
    assert not metadata.is_node_in_on_list('testNode2')

  def test_backup_list_funcs(self):
    metadata2 = FileMetadata('testFile1', '1', 100, 'testOrigin', [], [])
    assert not metadata2.is_node_in_backup_list('testNode')

    metadata2.add_node_to_backup_list('1', 'testNode')
    assert metadata2.is_node_in_backup_list('testNode')

    metadata2.add_node_to_backup_list('1', 'testNode2')
    assert metadata2.is_node_in_backup_list('testNode2')

    metadata2.remove_node_from_backup_list('testNode2')
    assert not metadata2.is_node_in_backup_list('testNode2')
    assert metadata2.is_node_in_backup_list('testNode')

  def test_on_list_funcs(self):
    metadata2 = FileMetadata('testFile1', '1', 100, 'testOrigin', [], [])
    assert not metadata2.is_node_in_on_list('testNode')

    metadata2.add_node_to_on_list('1', 'testNode')
    assert metadata2.is_node_in_on_list('testNode')

    metadata2.add_node_to_on_list('1', 'testNode2')
    assert metadata2.is_node_in_on_list('testNode2')

    metadata2.remove_node_from_on_list('testNode2')
    assert not metadata2.is_node_in_on_list('testNode2')
    assert metadata2.is_node_in_on_list('testNode')