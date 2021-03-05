import pytest
from ndn_distributed_repo.global_view.node_metadata import NodeMetadata

class TestNodeMetadata:
  def test_favor_funcs(self):
    metadata = NodeMetadata('testNodeId', 100, set(), set())
    assert metadata.get_favor() == 100
    metadata.set_favor(50)
    assert metadata.get_favor() == 50

  def test_backup_list_funcs(self):
    metadata = NodeMetadata('testNodeId', 50, set(), set())
    assert not metadata.is_file_in_backup_list('testFile')

    metadata.add_file_to_backup_list('testFile')
    assert metadata.is_file_in_backup_list('testFile')

    metadata.add_file_to_backup_list('testFile2')
    assert metadata.is_file_in_backup_list('testFile2')
    #check previously added file still present
    assert metadata.is_file_in_backup_list('testFile')

    metadata.remove_file_from_backup_list('testFile')
    assert not metadata.is_file_in_backup_list('testFile')
    #check previously added file still present
    assert metadata.is_file_in_backup_list('testFile2')

    metadata.remove_file_from_backup_list('testFile2')
    assert not metadata.is_file_in_backup_list('testFile2')

    with pytest.raises(Exception):
      metadata.remove_file_from_backup_list('testFile')

  def test_on_list_funcs(self):
    metadata = NodeMetadata('testNodeId', 50, set(), set())
    assert not metadata.is_file_in_on_list('testFile')

    metadata.add_file_to_on_list('testFile')
    assert metadata.is_file_in_on_list('testFile')

    metadata.add_file_to_on_list('testFile2')
    assert metadata.is_file_in_on_list('testFile2')
    #check previously added file still present
    assert metadata.is_file_in_on_list('testFile')

    metadata.remove_file_from_on_list('testFile')
    assert not metadata.is_file_in_on_list('testFile')
    #check previously added file still present
    assert metadata.is_file_in_on_list('testFile2')

    metadata.remove_file_from_on_list('testFile2')
    assert not metadata.is_file_in_on_list('testFile2')

    with pytest.raises(Exception):
      metadata.remove_file_from_on_list('testFile')