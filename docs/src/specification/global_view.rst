Global View
===========

Introduction
------------

Hydra does not have a “master” node or a group of nodes with special roles to maintain the status
of the system, including the files, sessions, and the mappings from file copies to sessions.
Instead, every node has a “global view” of the whole system, and they synchronize their global
views using the ``NDN State Vector Sync (SVS) protocol``.

"A node's view of the system", The Global View is a database that holds all information derived from
group messages. It is consistently updated group messages are heard.

The current global_view module stores data in an SQLite database.

Tables
------

The database has 5 tables.

* sessions:
    * ``id``:          session_id
    * ``node_name``
    * ``expire_at``:    time session expires. constantly updated via group messages
    * ``favor``:        higher value means the session is more willing to fetch and store files
    * ``state_vector``: state number in SVS for this session
    * ``is_expired``:   set to 1 once the session expires
* insertions: (each successful insert command will add a insertion)
    * ``id``: insertion_id
    * ``file_name``: file_name, including the path inside the repo. (file_name) is also the global prefix of the file that can be retrieved from the repo. If a file has multiple segments, “/file-name/seg-n” would be the name of individual packets.
    * ``sequence_number``: only if the insertion(file_name, sequence_number) is deleted can the insertion(file_name, (sequence_number+1) ) be acceptable
    * ``desired_copies``:  desired number of copies
    * ``packets``: number of packets (segments)
    * ``digests``: digest of each packet (segment) for ensuring data integrity
    * ``size``: total size of the file in bytes
    * ``origin_session_id``: id of the session which accepts the repo protocol Insert command
    * ``fetch_path``: path to fetch the file from the user during insertion
    * ``state_vector``
    * ``is_deleted``: set to 1 once the insertion is deleted
* stored_by: mapping from insertion i to session s if s has fetched and stored i
    * ``insertion_id``
    * ``session_id``
* backuped_by: mapping from insertion i to session s if s in on the backup list of i
    * ``insertion_id``
    * ``session_id``
    * ``rank``: rank on the backup list, starting from 0
    * ``nonce``
* pending_stores: store relationship will be put here if the insertion has not been added due to SVS delay, once the insertion is added later, any pending_stores belong to it will be added to stored_by
    * ``insertion_id``
    * ``session_id``

Reference
---------

The global_view module provides APIs including:

.. code-block:: bash

    * get_session(session_id: str)
    * get_sessions(including_expired: bool)
    * get_sessions_expired_by(timestamp: int)
    * update_session(session_id, node_name: str, expire_at: int, favor: int, state_vector: int)
    * expire_session(session_id)
    * get_insertion(insertion_id)
    * get_insertions(including_deleted: bool)
    * get_insertion_by_file_name(file_name: str)
    * get_underreplicated_insertions()
    * get_backupable_insertions()
    * add_insertion(insertion_id: str, file_name: str, sequence_number: int, size: int, origin_session_id: str, fetch_path: str, state_vector: int, digests: bytes, packets: int, desired_copies: int)
    * delete_insertion(insertion_id: str)
    * store_file(insertion_id: str, session_id: str)
    * get_stored_bys(insertion_id: str)
    * set_backups(insertion_id: str, backup_list: list[tuple[str, str]])
    * add_backup(insertion_id: str, session_id: str, rank: int, nonce: str)
    * get_backuped_bys(insertion_id: str)
    * get_pending_stores(insertion_id: str)
    * add_pending_store(insertion_id: str, session_id: str)

