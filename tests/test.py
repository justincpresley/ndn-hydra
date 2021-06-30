from global_view import GlobalView
import json

gv = GlobalView("/home/zixuan/ndn/repo/ndn_distributed_repo/global_view/test.db")

#seeding
gv.add_session("s_b079", "n_00", 1617831120, 1.85, 15)
gv.add_session("s_ec2f", "n_02", 1617921039, 1.98,  9)
gv.add_session("s_3c81", "n_01", 1617923012, 1.92, 12)
gv.add_session("s_0004", "n_04", 1617899994, 1.94,  4)
gv.add_session("s_0005", "n_05", 1617899995, 1.95,  5)
gv.add_session("s_0006", "n_06", 1617899996, 1.96,  6)
gv.add_session("s_0007", "n_07", 1617899997, 1.97,  7)
gv.add_session("s_0008", "n_08", 1617899998, 1.98,  8)
gv.add_session("s_0009", "n_09", 1617899999, 1.99,  9)

gv.add_insertion('i_385e', "/foo/bar.txt%9", 4, 1090, "s_b079", "/c/foo/bar.txt", 8, 5, 0)
gv.add_insertion('i_1289', "/foo/car.bin%1", 0,  208, "s_ec2f", "/t/foo/car.bin", 3, 1, 0)
gv.add_insertion('i_0003', "/foo/r3rrv.t%9", 3,  500, "s_0004", "/cs4/a/r3rrv.t", 4, 2, 4)

gv.store_file("i_0003", "s_0004")
gv.store_file("i_0003", "s_0008")
gv.store_file("i_0003", "s_ec2f")

gv.set_backups("i_0003", [("s_0007", "c724"), ("s_b079", "37e9"), ("s_0006", "1c2f")])

sessions = gv.get_sessions()
# print(json.dumps(sessions))

insertions = gv.get_insertions()
# print(json.dumps(insertions))

backupable_insertions = gv.get_backupable_insertions()
print(json.dumps(backupable_insertions))