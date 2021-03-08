
import pytest
from ndn_distributed_repo.global_view.global_view import GlobalView 

class TestGlobalView:
  def test_insert_file(self):
    gv = GlobalView()

    #check basic insert
    gv.insert_file("node1", 1, "1", "testFile", 100, [], [], 50)
    assert "node1" in gv.node_metadata_dict
    assert "testFile" in gv.file_metadata_dict

    assert gv.node_metadata_dict["node1"].get_favor() == 50
    assert gv.state_vector.get("node1") == 1

    assert [] == gv.file_metadata_dict["testFile"].get_on_list()
    assert [] == gv.file_metadata_dict["testFile"].get_backup_list()
    
    gv.insert_file("node1", 2, "1", "testFile2", 100, ["node2", "node3"], ["node4", "node5"], 60)
    assert "node1" in gv.node_metadata_dict
    assert "node2" in gv.node_metadata_dict
    assert "node3" in gv.node_metadata_dict
    assert "node4" in gv.node_metadata_dict
    assert "node5" in gv.node_metadata_dict

    assert "testFile" in gv.file_metadata_dict
    assert "testFile2" in gv.file_metadata_dict

    assert "testFile2" in gv.node_metadata_dict["node2"].get_on_list_file_set()
    assert "testFile2" in gv.node_metadata_dict["node3"].get_on_list_file_set()
    assert "testFile2" in gv.node_metadata_dict["node4"].get_backup_list_file_set()
    assert "testFile2" in gv.node_metadata_dict["node5"].get_backup_list_file_set()

    assert ["node2", "node3"] == gv.file_metadata_dict["testFile2"].get_on_list()
    assert ["node4", "node5"] == gv.file_metadata_dict["testFile2"].get_backup_list()

    assert gv.node_metadata_dict["node1"].get_favor() == 60
    assert gv.state_vector.get("node1") == 2

    #test reinserting same file with same insertion id
    with pytest.raises(Exception):
      gv.insert_file("node1", 4, "1", "testFile2", 100, ["node2", "node7", "node4"], ["node3", "node5", "node8"], 60)
    assert "node7" not in gv.node_metadata_dict
    assert "node8" not in gv.node_metadata_dict

    #test reinserting same file before deletion
    with pytest.raises(Exception):
      gv.insert_file("node1", 4, "2", "testFile2", 100, ["node2", "node7", "node4"], ["node3", "node5", "node8"], 60)
    assert "node7" not in gv.node_metadata_dict
    assert "node8" not in gv.node_metadata_dict

    #test reinserting file after deletion
    gv.delete_file("node1", 3, '1', "testFile2", 70)

    #test reinserting same file with same insertion id
    with pytest.raises(Exception):
      gv.insert_file("node1", 4, "1", "testFile2", 100, ["node2", "node7", "node4"], ["node3", "node5", "node8"], 60)
    assert "node7" not in gv.node_metadata_dict
    assert "node8" not in gv.node_metadata_dict
    assert "testFile2" in gv.file_metadata_dict
    assert gv.file_metadata_dict["testFile2"].is_file_deleted()

    gv.insert_file("node1", 4, "2", "testFile2", 100, ["node2", "node6", "node4"], ["node3", "node5", "node7"], 80)

    assert "node1" in gv.node_metadata_dict
    assert "node2" in gv.node_metadata_dict
    assert "node3" in gv.node_metadata_dict
    assert "node4" in gv.node_metadata_dict
    assert "node5" in gv.node_metadata_dict
    assert "node6" in gv.node_metadata_dict
    assert "node7" in gv.node_metadata_dict

    assert "testFile" in gv.file_metadata_dict
    assert "testFile2" in gv.file_metadata_dict

    assert "testFile2" in gv.node_metadata_dict["node2"].get_on_list_file_set()
    assert "testFile2" in gv.node_metadata_dict["node6"].get_on_list_file_set()
    assert "testFile2" in gv.node_metadata_dict["node4"].get_on_list_file_set()
    assert "testFile2" in gv.node_metadata_dict["node3"].get_backup_list_file_set()
    assert "testFile2" in gv.node_metadata_dict["node5"].get_backup_list_file_set()
    assert "testFile2" in gv.node_metadata_dict["node7"].get_backup_list_file_set()

    assert ["node2", "node6", "node4"] == gv.file_metadata_dict["testFile2"].get_on_list()
    assert ["node3", "node5", "node7"] == gv.file_metadata_dict["testFile2"].get_backup_list()

    assert gv.node_metadata_dict["node1"].get_favor() == 80
    assert gv.state_vector.get("node1") == 4

  def test_delete_file(self):
    gv = GlobalView()
    gv.insert_file("node1", 1, "1", "testFile", 100, ["node2", "node3"], ["node4", "node5"], 60)

    #check exception thrown when incorrect insertion_id
    with pytest.raises(Exception):
      gv.delete_file("node2", 1, '2', "testFile2", 80)

    #correctly formed delete
    gv.delete_file("node2", 1, '1', "testFile", 80)
    
    assert "testFile" in gv.file_metadata_dict
    assert gv.file_metadata_dict["testFile"].is_file_deleted()

    assert not gv.node_metadata_dict["node2"].get_on_list_file_set()
    assert not gv.node_metadata_dict["node3"].get_on_list_file_set()
    assert not  gv.node_metadata_dict["node4"].get_backup_list_file_set()
    assert not  gv.node_metadata_dict["node5"].get_backup_list_file_set()

    #check exception thrown when file not in global view
    with pytest.raises(Exception):
      gv.delete_file("node2", 1, '1', "testFile2", 80)

    gv.insert_file("node1", 1, "2", "testFile", 100, ["node2", "node3"], ["node4", "node5"], 60)

    #check exception thrown when inactive insertion_id
    with pytest.raises(Exception):
      gv.delete_file("node2", 1, '1', "testFile", 80)

  def test_node_stored_file(self):
    gv = GlobalView()
    gv.insert_file("node1", 1, "1", "testFile", 100, ["node2", "node3"], ["node4", "node5"], 60)

    #check node not previously on the backup list
    gv.node_stored_file("node1", 2, "1", "testFile", 90)


    assert gv.state_vector.get("node1") == 2
    assert gv.node_metadata_dict["node1"].get_favor() == 90

    assert "testFile" in gv.node_metadata_dict["node1"].get_on_list_file_set()
    assert ["node2", "node3", "node1"] == gv.file_metadata_dict["testFile"].get_on_list()
    assert ["node4", "node5"] == gv.file_metadata_dict["testFile"].get_backup_list()

    #check node previously on backup list
    gv.node_stored_file("node4", 1, "1", "testFile", 70)
    assert "testFile" in gv.node_metadata_dict["node4"].get_on_list_file_set()
    assert "testFile" not in gv.node_metadata_dict["node4"].get_backup_list_file_set()
    assert ["node2", "node3", "node1", "node4"] == gv.file_metadata_dict["testFile"].get_on_list()
    assert ["node5"] == gv.file_metadata_dict["testFile"].get_backup_list()

    #test exception thrown for file not inserted in repo
    with pytest.raises(Exception):
      gv.node_stored_file("node4", 1, "1", "testFile2", 70)
      
    #test exception thrown for incorrect insertion id
    with pytest.raises(Exception):
      gv.node_stored_file("node5", 1, "2", "testFile2", 70)

  def test_node_claimed_file(self):
    gv = GlobalView()
    gv.insert_file("node1", 1, "1", "testFile", 100, ["node2", "node3"], ["node4", "node5"], 60)

    #check node not previously on the backup list
    gv.node_claimed_file("node1", 2, "1", "testFile", 90)

    assert gv.state_vector.get("node1") == 2
    assert gv.node_metadata_dict["node1"].get_favor() == 90

    assert "testFile" in gv.node_metadata_dict["node1"].get_backup_list_file_set()
    assert ["node2", "node3"] == gv.file_metadata_dict["testFile"].get_on_list()
    assert ["node4", "node5", "node1"] == gv.file_metadata_dict["testFile"].get_backup_list()

    #check node previously in on list
    gv.node_claimed_file("node2", 1, "1", "testFile", 70)
    assert "testFile" in gv.node_metadata_dict["node4"].get_backup_list_file_set()
    assert "testFile" not in gv.node_metadata_dict["node4"].get_on_list_file_set()
    assert ["node3"] == gv.file_metadata_dict["testFile"].get_on_list()
    assert ["node4", "node5", "node1", "node2"] == gv.file_metadata_dict["testFile"].get_backup_list()

    #test exception thrown for file not inserted in repo
    with pytest.raises(Exception):
      gv.node_claimed_file("node4", 1, "1", "testFile2", 70)
      
    #test exception thrown for incorrect insertion id
    with pytest.raises(Exception):
      gv.node_claimed_file("node5", 1, "2", "testFile2", 70)

  def test_remove_node(self):
    gv = GlobalView()
    gv.insert_file("node1", 1, "1", "testFile", 100, ["node2", "nodeToRemove"], ["node4", "node5"], 60)
    gv.insert_file("node1", 2, "1", "testFile2", 100, ["node2", "node3"], ["nodeToRemove", "node5"], 60)

    gv.remove_node("node1", 3, "nodeToRemove", 70)

    assert gv.state_vector.get("node1") == 3
    assert gv.node_metadata_dict["node1"].get_favor() == 70

    assert ["node2"] == gv.file_metadata_dict["testFile"].get_on_list()
    assert ["node4", "node5"] == gv.file_metadata_dict["testFile"].get_backup_list()
    assert ["node2", "node3"] == gv.file_metadata_dict["testFile2"].get_on_list()
    assert ["node5"] == gv.file_metadata_dict["testFile2"].get_backup_list()

    assert "nodeToRemove" not in gv.node_metadata_dict
    