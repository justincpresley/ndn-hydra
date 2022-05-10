# -------------------------------------------------------------
# NDN Hydra Global View
# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Date:    2021-01-25
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/justincpresley/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

import os
import sqlite3
from sqlite3 import Error
from typing import List, Tuple

sql_create_nodes_tables = """
CREATE TABLE IF NOT EXISTS nodes (
    node_name TEXT PRIMARY KEY,
    favor REAL NOT NULL,
    state_vector INTEGER NOT NULL,
    expired INTEGER NOT NULL
);
"""
sql_create_files_tables = """
CREATE TABLE IF NOT EXISTS files (
    file_name TEXT PRIMARY KEY,
    desired_copies INTEGER NOT NULL DEFAULT 3,
    packets INTEGER NOT NULL DEFAULT 1,
    packet_size INTEGER NOT NULL DEFAULT 8800,
    size INTEGER NOT NULL,
    origin_node_name TEXT NOT NULL,
    fetch_path TEXT NOT NULL,
    is_deleted INTEGER NOT NULL DEFAULT 0
);
""" # remove origin_node_name, fetch_path, and is_deleted
sql_create_stores_tables = """
CREATE TABLE IF NOT EXISTS stores (
    id INTEGER PRIMARY KEY,
    file_name TEXT NOT NULL,
    node_name NOT NULL
);
"""
sql_create_backups_tables = """
CREATE TABLE IF NOT EXISTS backups (
    id INTEGER PRIMARY KEY,
    file_name TEXT NOT NULL,
    node_name NOT NULL,
    rank INTEGER NOT NULL,
    nonce TEXT NOT NULL
);
"""
sql_create_pending_stores_tables = """
CREATE TABLE IF NOT EXISTS pending_stores (
    id INTEGER PRIMARY KEY,
    file_name TEXT NOT NULL,
    node_name NOT NULL
);
"""

class GlobalView:
    def __init__(self, db:str):
        self.db = os.path.expanduser(db)
        if len(os.path.dirname(self.db)) > 0 and not os.path.exists(os.path.dirname(self.db)):
            try:
                os.makedirs(os.path.dirname(self.db))
            except PermissionError:
                raise PermissionError(f'Could not create database directory: {self.db}') from None
            except FileExistsError:
                pass
        self.__create_tables()
    def __get_connection(self):
        try:
            return sqlite3.connect(self.db)
        except Error as e:
            print(e)
        return None
    def __execute_sql(self, sql:str):
        result = []
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
    def __execute_sql_qmark(self, sql:str, par:Tuple):
        result = []
        conn = self.__get_connection()
        if conn is not None:
            try:
                c = conn.cursor()
                c.execute(sql, par)
                conn.commit()
                result = c.fetchall()
            except Error as e:
                print(e)
            conn.close()
        return result
    def __create_tables(self):
        self.__execute_sql(sql_create_nodes_tables)
        self.__execute_sql(sql_create_files_tables)
        self.__execute_sql(sql_create_stores_tables)
        self.__execute_sql(sql_create_backups_tables)
        self.__execute_sql(sql_create_pending_stores_tables)

    def __rerank_backups(self, file_name:str, node_name:str):
        sql = """
        SELECT DISTINCT file_name, node_name, rank
        FROM backups
        WHERE (file_name = ?) AND (node_name = ?)
        """
        result = self.__execute_sql_qmark(sql, (file_name, node_name))
        rank = -1
        if len(result) == 1:
            rank = result[0][2]
        if rank != -1:
            sql = """
            UPDATE backups
            SET rank = rank - 1
            WHERE (file_name = ?) AND (rank > ?)
            """
            self.__execute_sql_qmark(sql, (file_name, rank))

    def get_node(self, node_name:str):
        sql = """
        SELECT DISTINCT node_name, favor, state_vector, expired
        FROM nodes
        WHERE node_name = ?
        """
        result = self.__execute_sql_qmark(sql, (node_name,))
        if len(result) != 1:
            return None
        return {
            'node_name': result[0][0],
            'favor': result[0][1],
            'state_vector': result[0][2],
            'expired': False if (result[0][3] == 0) else True
        }

    def get_nodes(self, include_expired:bool=False):
        if include_expired:
            sql = """
            SELECT DISTINCT node_name, favor, state_vector, expired
            FROM nodes
            """
        else:
            sql = """
            SELECT DISTINCT node_name, favor, state_vector, expired
            FROM nodes
            WHERE expired = 0
            """
        results = self.__execute_sql(sql)
        nodes = []
        for result in results:
            nodes.append({
                'node_name': result[0],
                'favor': result[1],
                'state_vector': result[2],
                'expired': False if (result[3] == 0) else True
            })
        return nodes

    def update_node(self, node_name:str, favor:float, state_vector:int):
        sql = """
        INSERT OR REPLACE INTO nodes
            (node_name, favor, state_vector, expired)
        VALUES
            (?, ?, ?, COALESCE((SELECT expired FROM nodes WHERE node_name = ?), 1))
        """
        self.__execute_sql_qmark(sql, (node_name, favor, state_vector, node_name))

    def renew_node(self, node_name:str):
        sql = """
        UPDATE nodes
        SET expired = 0
        WHERE node_name = ?
        """
        self.__execute_sql_qmark(sql, (node_name,))

    def expire_node(self, node_name:str):
        # stores
        sql = """
        DELETE FROM stores WHERE node_name = ?
        """
        self.__execute_sql_qmark(sql, (node_name,))
        # backups
        sql = """
        SELECT file_name, node_name
        FROM backups
        WHERE node_name = ?
        """
        backups = self.__execute_sql_qmark(sql, (node_name,))
        for backup in backups:
            # rerank
            self.__rerank_backups(backup[0], node_name)
        # remove
        sql = """
        DELETE FROM backups WHERE node_name = ?
        """
        self.__execute_sql_qmark(sql, (node_name,))
        # pending_stores
        sql = """
        DELETE FROM pending_stores WHERE node_name = ?
        """
        self.__execute_sql_qmark(sql, (node_name,))
        # expire node
        sql = """
        UPDATE nodes
        SET expired = 1
        WHERE node_name = ?
        """
        self.__execute_sql_qmark(sql, (node_name,))

    def __split_digests(self, digests:bytes, size:int):
        digests_bytes = bytes(digests)
        return [digests_bytes[i:i+size] for i in range(0, len(digests_bytes), size)]

    def get_file(self, file_name:str):
        sql = """
        SELECT DISTINCT
            file_name, desired_copies, packets, size, origin_node_name, fetch_path, is_deleted, packet_size
        FROM files
        WHERE file_name = ?
        """
        result = self.__execute_sql_qmark(sql, (file_name,))
        if len(result) != 1:
            return None
        return {
            'file_name': result[0][0],
            'desired_copies': result[0][1],
            'packets': result[0][2],
            'size': result[0][3],
            'origin_node_name': result[0][4],
            'fetch_path': result[0][5],
            'is_deleted': False if (result[0][6] == 0) else True,
            'packet_size': result[0][7],
            'stores': self.get_stores(result[0][0]),
            'backups': self.get_backups(result[0][0])
        }

    def get_files(self, including_deleted:bool=False):
        if including_deleted:
            sql = """
            SELECT DISTINCT
                file_name, desired_copies, packets, size, origin_node_name, fetch_path, is_deleted, packet_size
            FROM files
            """
        else:
            sql = """
            SELECT DISTINCT
                file_name, desired_copies, packets, size, origin_node_name, fetch_path, is_deleted, packet_size
            FROM files
            WHERE is_deleted = 0
            """
        results = self.__execute_sql(sql)
        files = []
        for result in results:
            files.append({
                'file_name': result[0],
                'desired_copies': result[1],
                'packets': result[2],
                'size': result[3],
                'origin_node_name': result[4],
                'fetch_path': result[5],
                'is_deleted': False if (result[6] == 0) else True,
                'packet_size': result[7],
                'stores': self.get_stores(result[0]),
                'backups': self.get_backups(result[0])
            })
        return files

    def get_underreplicated_files(self):
        files = self.get_files()
        underreplicated_files = []
        for file in files:
            if len(file['stores']) < file['desired_copies']:
                underreplicated_files.append(file)
        return underreplicated_files

    def get_backupable_files(self):
        files = self.get_files()
        backupable_files = []
        for file in files:
            if( len(file['stores']) + len(file['backups']) ) < (file['desired_copies'] * 2):
                backupable_files.append(file)
        return backupable_files

    def add_file(self, file_name:str, size:int, origin_node_name:str, fetch_path:str, packet_size:int, packets:int, desired_copies:int):
        sql = """
        INSERT OR IGNORE INTO files
            (file_name, desired_copies, packets, size, origin_node_name, fetch_path, is_deleted, packet_size)
        VALUES
            (?, ?, ?, ?, ?, ?, 0, ?)
        """
        self.__execute_sql_qmark(sql, (file_name, desired_copies, packets, size, origin_node_name, fetch_path, packet_size))

    def delete_file(self, file_name:str):
        # stores
        sql = """
        DELETE FROM stores WHERE file_name = ?
        """
        self.__execute_sql_qmark(sql, (file_name,))
        # backups
        sql = """
        DELETE FROM backups WHERE file_name = ?
        """
        self.__execute_sql_qmark(sql, (file_name,))
        # backups
        sql = """
        SELECT DISTINCT file_name, node_name
        FROM backups
        WHERE file_name = ?
        """
        backups = self.__execute_sql_qmark(sql, (file_name,))
        for backup in backups:
            # rerank
            self.__rerank_backups(file_name, backup[1])
        # remove
        sql = """
        DELETE FROM backups WHERE file_name = ?
        """
        self.__execute_sql_qmark(sql, (file_name,))
        # pending_stores
        sql = """
        DELETE FROM pending_stores WHERE file_name = ?
        """
        self.__execute_sql_qmark(sql, (file_name,))
        # insertions
        sql = """
        UPDATE files
        SET is_deleted = 1
        WHERE file_name = ?
        """
        self.__execute_sql_qmark(sql, (file_name,))

    def store_file(self, file_name:str, node_name:str):
        # rerank backuped_by
        self.__rerank_backups(file_name, node_name)
        # remove from backuped_by
        sql = """
        DELETE FROM backups WHERE (file_name = ?) AND (node_name = ?)
        """
        self.__execute_sql_qmark(sql, (file_name, node_name))
        # add to stored_by
        sql = """
        INSERT OR IGNORE INTO stores
            (file_name, node_name)
        VALUES
            (?, ?)
        """
        self.__execute_sql_qmark(sql, (file_name, node_name))

    def set_backups(self, file_name:str, backup_list:List[Tuple[str, str]]):
        # remove previous backups
        sql = """
        DELETE FROM backups WHERE (file_name = ?)
        """
        self.__execute_sql_qmark(sql, (file_name,))
        # add backups
        length = len(backup_list)
        for rank in range(length):
            backup = backup_list[rank]
            sql = """
            INSERT OR IGNORE INTO backups
                (file_name, node_name, rank, nonce)
            VALUES
                (?, ?, ?, ?)
            """
            self.__execute_sql_qmark(sql, (file_name, backup[0], rank, backup[1]))

    def add_backup(self, file_name:str, node_name:str, rank:int, nonce:str):
        # delete all backups with larger rank value
        sql = """
        DELETE FROM backups
        WHERE (file_name = ?) AND rank >= ?
        """
        self.__execute_sql_qmark(sql, (file_name, rank))
        # add this backup
        sql = """
        INSERT OR IGNORE INTO backups
            (file_name, node_name, rank, nonce)
        VALUES
            (?, ?, ?, ?)
        """
        self.__execute_sql_qmark(sql, (file_name, node_name, rank, nonce))

    def get_stores(self, file_name:str):
        sql = """
        SELECT DISTINCT file_name, node_name
        FROM stores
        WHERE file_name = ?
        ORDER BY node_name ASC
        """
        results = self.__execute_sql_qmark(sql, (file_name,))
        stores = []
        for result in results:
            stores.append(result[1])
        return stores

    def get_backups(self, file_name:str):
        sql = """
        SELECT DISTINCT file_name, node_name, rank, nonce
        FROM backups
        WHERE file_name = ?
        ORDER BY rank
        """
        results = self.__execute_sql_qmark(sql, (file_name,))
        backups = []
        for result in results:
            backups.append({
                'node_name': result[1],
                'rank': result[2],
                'nonce': result[3]
            })
        return backups

    def get_pending_stores(self, file_name:str):
        sql = """
        SELECT DISTINCT file_name, node_name
        FROM pending_stores
        WHERE file_name = ?
        """
        results = self.__execute_sql_qmark(sql, (file_name,))
        pending_stores = []
        for result in results:
            pending_stores.append(result[1])
        return pending_stores

    def add_pending_store(self, file_name:str, node_name:str):
        sql = """
        INSERT OR IGNORE INTO pending_stores
            (file_name, node_name)
        VALUES
            (?, ?)
        """
        self.__execute_sql_qmark(sql, (file_name, node_name))