from ndn.encoding import *
from .message_base import MessageBodyBase

class AddMessageBodyTypes:
    ID = 82
    NODE_ID = 83
    FAVOR = 84
    VALID_THRU = 85
    FILE = 90
    FILE_COPIES = 91
    FILE_SIZE = 92
    FILE_BLOCKS = 93
    FILE_ORIGINAL_PATH = 94
    FILE_SEQ = 95
    ON = 100
    BACKUP = 101

class FileTlv(TlvModel):
    name = NameField()
    copies = UintField(AddMessageBodyTypes.FILE_COPIES, default=3)
    size = UintField(AddMessageBodyTypes.FILE_SIZE)
    blocks = UintField(AddMessageBodyTypes.FILE_BLOCKS)

class FileOriginalPathTlv(TlvModel):
    name = NameField()

class AddMessageBodyTlv(TlvModel):
    insertion_id = UintField(AddMessageBodyTypes.ID)
    node_id = BytesField(AddMessageBodyTypes.NODE_ID)
    favor = UintField(AddMessageBodyTypes.FAVOR)
    valid_thru = UintField(AddMessageBodyTypes.VALID_THRU)
    file = ModelField(AddMessageBodyTypes.FILE, FileTlv)
    file_original_path = ModelField(AddMessageBodyTypes.FILE_ORIGINAL_PATH, FileOriginalPathTlv)
    file_seq = UintField(AddMessageBodyTypes.FILE_SEQ)
    on_list = RepeatedField(BytesField(AddMessageBodyTypes.ON))
    backup_list = RepeatedField(BytesField(AddMessageBodyTypes.BACKUP))  

class AddMessageBody(MessageBodyBase):
    def __init__(self, nid:str, seq:int, raw_bytes:bytes):
        super(AddMessageBody, self).__init__(nid, seq)
        self.add_message_body = AddMessageBodyTlv.parse(raw_bytes)

    async def apply(self, global_view):
        #TODO: apply this add msg to the global view
        return
