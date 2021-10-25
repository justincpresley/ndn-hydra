import sqlite3
import os
from base64 import b64decode, b64encode
from Cryptodome.Random import get_random_bytes

from ndn.security.tpm import TpmFile
from ndn.security.keychain import KeychainSqlite3
from ndn.app_support.security_v2 import self_sign
from ndn.app_support.security_v2 import KEY_COMPONENT
from ndn.encoding import Name, Component

conn = sqlite3.connect("/home/hydra_op/.ndn/pib.db")
tpm = TpmFile("/home/hydra_op/.ndn/ndnsec-key-file")
keychain = KeychainSqlite3("/home/hydra_op/.ndn/pib.db", tpm)

# generate key name
id_name = '/edu/ucla/cs/bruins/hydra_op'
name = Name.to_bytes(id_name)

cursor = conn.execute('SELECT identity_id, key_name FROM keys WHERE identity_id=' 
                        '(SELECT id FROM identities WHERE identity=?)', (name,))
data = cursor.fetchone()
if data is not None:
    print('delete existing key materials...')
    identity_id, key_name = data
    # delete certificates
    conn.execute('DELETE FROM certificates WHERE key_id='
                        '(SELECT id FROM keys WHERE identity_id=?)', (identity_id,))
    conn.commit()
    # delete keys 
    conn.execute('DELETE FROM keys WHERE identity_id=?', (identity_id,))
    conn.commit()
    # delete identity
    conn.execute('DELETE FROM identities WHERE id=?', (identity_id,))
    conn.commit()
    # delete prv key file
    tpm.delete_key(Name.normalize(key_name))

print('creating new key materials...')
key_name_bytes = Name.to_bytes(Name.from_str(id_name))
key_name = Name.normalize(key_name_bytes) + [KEY_COMPONENT, Component.from_bytes(get_random_bytes(8))]
if tpm.key_exist(key_name):
    print('key exist')
else:
    print(Name.to_str(key_name))

# load prv key
key_der = b''
with open('hydra_prv.der', 'rb') as f:
    while (byte := f.read(1)):
        key_der = key_der + byte

# save prv key
key_name = Name.encode(key_name)
key_b64 = tpm._base64_newline(b64encode(key_der))
file_name = os.path.join(tpm.path, tpm._to_file_name(key_name))
with open(file_name, 'wb') as f:
    f.write(key_b64)

# load pub key
key_der = b''
with open('hydra_pub.der', 'rb') as f:
    while (byte := f.read(1)):
        key_der = key_der + byte
# self-sign
signer = tpm.get_signer(key_name)
cert_name, cert_data = self_sign(key_name, key_der, signer)
cert_name = Name.to_bytes(cert_name)
print(Name.to_str(cert_name))

name = Name.to_bytes(id_name)
conn.execute('INSERT INTO identities (identity) VALUES (?)', (name,))
conn.commit()

conn.execute('INSERT INTO keys (identity_id, key_name, key_bits) VALUES (?, ?, ?)',
              (keychain.__getitem__(name).row_id, key_name, key_der))
conn.commit()

conn.execute('INSERT INTO certificates (key_id, certificate_name, certificate_data)'
             'VALUES ((SELECT id FROM keys WHERE key_name=?), ?, ?)',
             (key_name, cert_name, bytes(cert_data)))
conn.commit()