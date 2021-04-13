import os
import sqlite3
from sqlite3 import Error
from typing import List, Tuple

sql_create_sessions_tables = """
CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    node_name TEXT NOT NULL,
    expire_at INTEGER NOT NULL,
    favor REAL NOT NULL,
    state_vector INTEGER NOT NULL,
    is_expired INTEGER NOT NULL DEFAULT 0
);
"""
sql_create_insertions_tables = """
CREATE TABLE IF NOT EXISTS insertions (
    id TEXT PRIMARY KEY,
    file_name TEXT NOT NULL,
    sequence_number INTEGER NOT NULL,
    desired_copies INTEGER NOT NULL DEFAULT 3,
    packets INTEGER NOT NULL DEFAULT 1,
    size INTEGER NOT NULL,
    origin_session_id TEXT NOT NULL,
    fetch_path TEXT NOT NULL,
    state_vector INTEGER NOT NULL,
    is_deleted INTEGER NOT NULL DEFAULT 0
);
"""
sql_create_stored_by_tables = """
CREATE TABLE IF NOT EXISTS stored_by (
    id INTEGER PRIMARY KEY,
    insertion_id TEXT NOT NULL,
    session_id NOT NULL
);
"""
sql_create_backuped_by_tables = """
CREATE TABLE IF NOT EXISTS backuped_by (
    id INTEGER PRIMARY KEY,
    insertion_id TEXT NOT NULL,
    session_id NOT NULL,
    rank INTEGER NOT NULL,
    nonce TEXT NOT NULL
);
"""
sql_create_pending_stores_tables = """
CREATE TABLE IF NOT EXISTS pending_stores (
    id INTEGER PRIMARY KEY,
    insertion_id TEXT NOT NULL,
    session_id NOT NULL
);
"""

class GlobalView:

    def __init__(self, db: str):
        self.db = os.path.expanduser(db)
        if len(os.path.dirname(self.db)) > 0 and not os.path.exists(os.path.dirname(self.db)):
            try:
                os.makedirs(os.path.dirname(self.db))
            except PermissionError:
                raise PermissionError(f'Could not create database directory: {self.db}') from None
        self.__create_tables()

    def __get_connection(self):
        conn = None
        try:
            conn = sqlite3.connect(self.db)
        except Error as e:
            print(e)
        return conn

    def __execute_sql(self, sql):
        # print(sql)
        result = None
        conn = self.__get_connection()
        if conn is not None:
            try:
                c = conn.cursor()
                c.execute(sql)
                conn.commit()
                result = c.fetchall()
            except Error as e:
                print(e)
            conn.close()
        return result

    def __create_tables(self):
        self.__execute_sql(sql_create_sessions_tables)
        self.__execute_sql(sql_create_insertions_tables)
        self.__execute_sql(sql_create_stored_by_tables)
        self.__execute_sql(sql_create_backuped_by_tables)
        self.__execute_sql(sql_create_pending_stores_tables)

    def __rerank_backuped_by(self, insertion_id: str, session_id: str):
        # check rank
        ps_get_backup = """
        SELECT DISTINCT insertion_id, session_id, rank
        FROM backuped_by
        WHERE (insertion_id = '{insertion_id}') AND (session_id = '{session_id}')
        """
        sql_get_backup = ps_get_backup.format(insertion_id=insertion_id, session_id=session_id)
        result = self.__execute_sql(sql_get_backup)
        # return result
        rank = -1
        if len(result) == 1:
            rank = result[0][2]
        if rank != -1:
            ps_rerank = """
            UPDATE backuped_by
            SET rank = rank - 1
            WHERE (insertion_id = '{insertion_id}') AND (rank > {rank})
            """
            sql_rerank = ps_rerank.format(insertion_id=insertion_id, rank=rank)
            self.__execute_sql(sql_rerank)

    def get_session(self, session_id: str):
        ps = """
        SELECT DISTINCT 
            id, node_name, expire_at, favor, state_vector, is_expired 
        FROM 
            sessions
        WHERE
            id = '{session_id}'
        """
        sql = ps.format(session_id=session_id)
        result = self.__execute_sql(sql)
        if len(result) != 1:
            return None
        else:
            return {
                'id': result[0][0],
                'node_name': result[0][1],
                'expire_at': result[0][2],
                'favor': result[0][3],
                'state_vector': result[0][4],
                'is_expired': False if (result[0][5] == 0) else True
            }

    def get_sessions(self, including_expired: bool = False):
        if including_expired:
            sql = """SELECT DISTINCT id, node_name, expire_at, favor, state_vector, is_expired FROM sessions"""
        else:
            sql = """
            SELECT DISTINCT 
                id, node_name, expire_at, favor, state_vector, is_expired
            FROM 
                sessions
            WHERE
                is_expired = 0
            """
        results = self.__execute_sql(sql)
        sessions = []
        for result in results:
            sessions.append({
                'id': result[0],
                'node_name': result[1],
                'expire_at': result[2],
                'favor': result[3],
                'state_vector': result[4],
                'is_expired': False if (result[5] == 0) else True
            })
        return sessions

    def get_sessions_expired_by(self, timestamp: int):
        ps = """
        SELECT DISTINCT 
            id, node_name, expire_at, favor, state_vector, is_expired 
        FROM
            sessions
        WHERE
            is_expired = 0 AND
            expire_at <= {timestamp}
        """
        sql = ps.format(timestamp=timestamp)
        results = self.__execute_sql(sql)
        sessions = []
        for result in results:
            sessions.append({
                'id': result[0],
                'node_name': result[1],
                'expire_at': result[2],
                'favor': result[3],
                'state_vector': result[4],
                'is_expired': False if (result[5] == 0) else True
            })
        return sessions

    def __add_session(self, session_id: str, node_name: str, expire_at: int, favor: float, state_vector: int):
        # start session
        ps = """
        INSERT OR IGNORE INTO sessions
            (id, node_name, expire_at, favor, state_vector, is_expired)
        VALUES
            ('{id}', '{node_name}', {expire_at}, {favor:.4f}, {state_vector}, 0)
        """
        sql = ps.format(
            id=session_id,
            node_name=node_name,
            expire_at=expire_at,
            favor=favor,
            state_vector=state_vector
        )
        self.__execute_sql(sql)

    def update_session(self, session_id: str, node_name: str, expire_at: int, favor: float, state_vector: int):
        self.__add_session(session_id, node_name, expire_at, favor, state_vector)
        ps = """
        UPDATE sessions
        SET expire_at = {expire_at},
            favor = {favor},
            state_vector = {state_vector}
        WHERE
            id = '{session_id}'
        """
        sql = ps.format(session_id=session_id, expire_at=expire_at, favor=favor, state_vector=state_vector)
        self.__execute_sql(sql)

    def expire_session(self, session_id: str):
        
        # stored_by
        ps_stored_by = """
        DELETE FROM stored_by WHERE session_id = '{session_id}'
        """
        sql_stored_by = ps_stored_by.format(session_id=session_id)
        self.__execute_sql(sql_stored_by)
        
        # backuped_by
        ps_get_backups = """
        SELECT insertion_id, session_id
        FROM backuped_by
        WHERE session_id = '{session_id}'
        """
        sql_get_backups = ps_get_backups.format(session_id=session_id)
        backups = self.__execute_sql(sql_get_backups)
        for backup in backups:
            # rerank
            self.__rerank_backuped_by(backup[0], session_id)
        # remove
        ps_delete_backuped_by = """
        DELETE FROM backuped_by WHERE session_id = '{session_id}'
        """
        sql_delete_backuped_by = ps_delete_backuped_by.format(session_id=session_id)
        self.__execute_sql(sql_delete_backuped_by)

        # pending_stores
        ps_pending_stores = """
        DELETE FROM pending_stores WHERE session_id = '{session_id}'
        """
        sql_pending_stores = ps_pending_stores.format(session_id=session_id)
        self.__execute_sql(sql_pending_stores)

        # expire session
        ps = """
        UPDATE sessions
        SET is_expired = 1
        WHERE id = '{session_id}'
        """
        sql = ps.format(session_id=session_id)
        self.__execute_sql(sql)

    def get_insertion(self, insertion_id: str):
        ps = """
        SELECT DISTINCT 
            id, file_name, sequence_number, desired_copies, packets, size, origin_session_id, fetch_path, state_vector, is_deleted
        FROM 
            insertions
        WHERE
            id = '{insertion_id}'
        """
        sql = ps.format(insertion_id=insertion_id)
        result = self.__execute_sql(sql)
        if len(result) != 1:
            return None
        else:
            return {
                'id': result[0][0],
                'file_name': result[0][1],
                'sequence_number': result[0][2],
                'desired_copies': result[0][3],
                'packets': result[0][4],
                'size': result[0][5],
                'origin_session_id': result[0][6],
                'fetch_path': result[0][7],
                'state_vector': result[0][8],
                'is_deleted': False if (result[0][9] == 0) else True,
                'stored_bys': self.get_stored_bys(result[0][0]),
                'backuped_bys': self.get_backuped_bys(result[0][0])
            }

    def get_insertions(self, including_deleted: bool = False):
        if including_deleted:
            sql = """
            SELECT DISTINCT 
                id, file_name, sequence_number, desired_copies, packets, size, origin_session_id, fetch_path, state_vector, is_deleted
            FROM 
                insertions
            """
        else:
            sql = """
            SELECT DISTINCT 
                id, file_name, sequence_number, desired_copies, packets, size, origin_session_id, fetch_path, state_vector, is_deleted
            FROM 
                insertions
            WHERE
                is_deleted = 0
            """
        results = self.__execute_sql(sql)
        if results == None:
            return []
        insertions = []
        for result in results:
            insertions.append({
                'id': result[0],
                'file_name': result[1],
                'sequence_number': result[2],
                'desired_copies': result[3],
                'packets': result[4],
                'size': result[5],
                'origin_session_id': result[6],
                'fetch_path': result[7],
                'state_vector': result[8],
                'is_deleted': False if (result[9] == 0) else True,
                'stored_bys': self.get_stored_bys(result[0]),
                'backuped_bys': self.get_backuped_bys(result[0])
            })
        return insertions

    def get_underreplicated_insertions(self):
        insertions = self.get_insertions()
        underreplicated_insertions = []
        for insertion in insertions:
            if len(insertion['stored_bys']) < insertion['desired_copies']:
                underreplicated_insertions.append(insertion)
        return underreplicated_insertions

    def get_backupable_insertions(self):
        insertions = self.get_insertions()
        backupable_insertions = []
        for insertion in insertions:
            if (len(insertion['stored_bys']) + len(insertion['backuped_bys'])) < (insertion['desired_copies'] * 2):
                backupable_insertions.append(insertion)
        return backupable_insertions

    def add_insertion(self, insertion_id: str, file_name: str, sequence_number: int, size: int, origin_session_id: str,
               fetch_path: str, state_vector: int, packets=1, desired_copies=3):
        # # check (same insertion_id):
        # insertion = self.get_insertion(insertion_id)
        # if (insertion is not None) and (insertion['is_deleted'] == False):
        #     return False
        # # TODO(check if there are insertions with the same file_name)
        # # TODO(may not needed, check if there are insertions with the same file_name and same sequence_number)

        ps = """
        INSERT OR IGNORE INTO insertions 
            (id, file_name, sequence_number, desired_copies, packets, size, origin_session_id, fetch_path, state_vector, is_deleted)
        VALUES
            ('{id}', '{file_name}', {sequence_number}, {desired_copies}, {packets}, {size}, '{origin_session_id}', '{fetch_path}', {state_vector}, 0)
        """

        sql = ps.format(
            id=insertion_id,
            file_name=file_name,
            sequence_number=sequence_number,
            desired_copies=desired_copies,
            packets=packets,
            size=size,
            origin_session_id=origin_session_id,
            fetch_path=fetch_path,
            state_vector=state_vector
        )
        self.__execute_sql(sql)

    def delete_insertion(self, insertion_id: str):
        # stored_by
        ps_stored_by = """
        DELETE FROM stored_by WHERE insertion_id = '{insertion_id}'
        """
        sql_stored_by = ps_stored_by.format(insertion_id=insertion_id)
        self.__execute_sql(sql_stored_by)
        
        # backuped_by
        ps_backuped_by = """
        DELETE FROM backuped_by WHERE insertion_id = '{insertion_id}'
        """
        sql_backuped_by = ps_backuped_by.format(insertion_id=insertion_id)
        self.__execute_sql(sql_backuped_by)

        # backuped_by
        ps_get_backups = """
        SELECT insertion_id, session_id
        FROM backuped_by
        WHERE insertion_id = '{insertion_id}'
        """
        sql_get_backups = ps_get_backups.format(insertion_id=insertion_id)
        backups = self.__execute_sql(sql_get_backups)
        for backup in backups:
            # rerank
            self.__rerank_backuped_by(insertion_id, backup[1])
        # remove
        ps_delete_backuped_by = """
        DELETE FROM backuped_by WHERE insertion_id = '{insertion_id}'
        """
        sql_delete_backuped_by = ps_delete_backuped_by.format(insertion_id=insertion_id)
        self.__execute_sql(sql_delete_backuped_by)

        # pending_stores
        ps_pending_stores = """
        DELETE FROM pending_stores WHERE insertion_id = '{insertion_id}'
        """
        sql_pending_stores = ps_pending_stores.format(insertion_id=insertion_id)
        self.__execute_sql(sql_pending_stores)

        # insertions
        ps_insertions = """
        UPDATE insertions
        SET is_deleted = 1
        WHERE id = '{insertion_id}'
        """
        sql_insertions = ps_insertions.format(insertion_id=insertion_id)
        self.__execute_sql(sql_insertions)

    def store_file(self, insertion_id: str, session_id: str):
        # rerank backuped_by
        self.__rerank_backuped_by(insertion_id, session_id)
        # remove from backuped_by
        ps_delete_backuped_by = """
        DELETE FROM backuped_by WHERE (insertion_id = '{insertion_id}') AND (session_id = '{session_id}')
        """
        sql_delete_backuped_by = ps_delete_backuped_by.format(insertion_id=insertion_id, session_id=session_id)
        self.__execute_sql(sql_delete_backuped_by)
        # add to stored_by
        ps_add_to_stored_by = """
        INSERT OR IGNORE INTO stored_by
            (insertion_id, session_id)
        VALUES
            ('{insertion_id}', '{session_id}')
        """
        sql_add_to_stored_by = ps_add_to_stored_by.format(insertion_id=insertion_id, session_id=session_id)
        self.__execute_sql(sql_add_to_stored_by)

    def set_backups(self, insertion_id: str, backup_list: List[Tuple[str, str]]):
        # remove previous backups
        ps_delete_backuped_by = """
        DELETE FROM backuped_by WHERE (insertion_id = '{insertion_id}')
        """
        sql_delete_backuped_by = ps_delete_backuped_by.format(insertion_id=insertion_id)
        self.__execute_sql(sql_delete_backuped_by)
        # add backups
        length = len(backup_list)
        for rank in range(length):
            backup = backup_list[rank]
            ps_add_backup = """
            INSERT OR IGNORE INTO backuped_by
                (insertion_id, session_id, rank, nonce)
            VALUES
                ('{insertion_id}', '{session_id}', {rank}, '{nonce}') 
            """
            sql_add_backup = ps_add_backup.format(insertion_id=insertion_id, session_id=backup[0], rank=rank, nonce=backup[1])
            self.__execute_sql(sql_add_backup)

    def add_backup(self, insertion_id: str, session_id: str, rank: int, nonce: str):
        # delete all backups with larger rank value
        ps_delete_backuped_by = """
        DELETE FROM backuped_by 
        WHERE (insertion_id = '{insertion_id}') AND rank >= {rank}
        """
        sql_delete_backuped_by = ps_delete_backuped_by.format(insertion_id=insertion_id, rank=rank)
        self.__execute_sql(sql_delete_backuped_by)
        # add this backup
        ps_add_backup = """
        INSERT OR IGNORE INTO backuped_by
            (insertion_id, session_id, rank, nonce)
        VALUES
            ('{insertion_id}', '{session_id}', {rank}, '{nonce}') 
        """
        sql_add_backup = ps_add_backup.format(insertion_id=insertion_id, session_id=session_id, rank=rank, nonce=nonce)
        self.__execute_sql(sql_add_backup)

    def get_stored_bys(self, insertion_id: str):
        ps = """
        SELECT DISTINCT insertion_id, session_id
        FROM stored_by
        WHERE insertion_id = '{insertion_id}'
        """
        sql = ps.format(insertion_id=insertion_id)
        results = self.__execute_sql(sql)
        if results == None:
            return []
        stored_bys = []
        for result in results:
            stored_bys.append(result[1])
        return stored_bys

    def get_backuped_bys(self, insertion_id: str):
        ps = """
        SELECT DISTINCT insertion_id, session_id, rank, nonce
        FROM backuped_by
        WHERE insertion_id = '{insertion_id}'
        ORDER BY rank
        """
        sql = ps.format(insertion_id=insertion_id)
        results = self.__execute_sql(sql)
        if results == None:
            return []
        backuped_bys = []
        for result in results:
            backuped_bys.append({
                'session_id': result[1],
                'rank': result[2],
                'nonce': result[3]
            })
        return backuped_bys

    def get_pending_stores(self, insertion_id: str):
        ps = """
        SELECT DISTINCT insertion_id, session_id
        FROM pending_stores
        WHERE insertion_id = '{insertion_id}'
        """
        sql = ps.format(insertion_id=insertion_id)
        results = self.__execute_sql(sql)
        if results == None:
            return []
        pending_stores = []
        for result in results:
            pending_stores.append(result[1])
        return pending_stores   