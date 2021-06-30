import os
import sqlite3
from sqlite3 import Error
from typing import List, Tuple

from ndn.encoding.name import Component, Name

class IsFetchedType:
    PACKET_NOT_FETCHED = 0
    PACKET_FETCHED = 1
    FILE_FETCHED = 2

sql_create_metainfos_tables = """
CREATE TABLE IF NOT EXISTS metainfos (
    id INTEGER PRIMARY KEY,
    insertion_id TEXT NOT NULL,
    file_name TEXT NOT NULL,
    packets INTEGER NOT NULL DEFAULT 1,
    packet INTEGER NOT NULL,
    digest BLOB NOT NULL,
    is_fetched INTEGER NOT NULL DEFAULT 0,
    k TEXT NOT NULL,
    fetch_path TEXT NOT NULL,
    UNIQUE (insertion_id, packet)
);
"""
sql_create_kvs_tables = """
CREATE TABLE IF NOT EXISTS kvs (
    k TEXT PRIMARY KEY,
    v BLOB NOT NULL
);
"""
sql_create_announcements_tables = """
CREATE TABLE IF NOT EXISTS announcements (
    id INTEGER PRIMARY KEY,
    insertion_id TEXT NOT NULL,
    is_announced INTEGER NOT NULL DEFAULT 0
);
"""


class DataStorage:

    def __init__(self, db: str):
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
        conn = None
        try:
            conn = sqlite3.connect(self.db)
        except Error as e:
            print(e)
        return conn

    def __execute_sql(self, sql: str):
        # print(sql)
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

    def __execute_sql_qmark(self, sql: str, par: Tuple):
        # print(sql)
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
        self.__execute_sql(sql_create_metainfos_tables)
        self.__execute_sql(sql_create_kvs_tables)
        self.__execute_sql(sql_create_announcements_tables)

    def add_metainfos(self, insertion_id: str, file_name: str, packets: int, digests: List[bytes], fetch_path: str):
        if len(digests) != packets:
            return
        for i in range(packets):
            key = Name.normalize(file_name) + [Component.from_segment(i)]
            key_str = Name.to_str(key)
            self.add_metainfo(insertion_id, file_name, packets, i, digests[i], key_str, fetch_path)

    def add_metainfo(self, insertion_id: str, file_name: str, packets: int, packet: int, digest: bytes, key: str, fetch_path: str):
        sql = """
        INSERT OR IGNORE INTO metainfos
            (insertion_id, file_name, packets, packet, digest, is_fetched, k, fetch_path)
        VALUES
            (?, ?, ?, ?, ?, 0, ?, ?)
        """
        self.__execute_sql_qmark(sql, (insertion_id, file_name, packets, packet, digest, key, fetch_path))

    def get_fetchable_metainfos(self):
        sql = """
        SELECT DISTINCT
            insertion_id, file_name, packets, packet, digest, is_fetched, k, fetch_path
        FROM
            metainfos
        WHERE
            is_fetched = 0
        """
        results = self.__execute_sql(sql)
        metainfos = []
        for result in results:
            metainfos.append({
                'insertion_id': result[0],
                'file_name': result[1],
                'packets': result[2],
                'packet': result[3],
                'digest': bytes(result[4]),
                'is_fetched': result[5],
                'key': result[6],
                'fetch_path': result[7]
            })
        return metainfos

    def __update_metainfo_file_fetched(self, insertion_id: str):
        sql_get_metainfos = """
        SELECT DISTINCT
            packet, packets
        FROM
            metainfos
        WHERE
            insertion_id = ? AND
            is_fetched = 1
        """
        results = self.__execute_sql_qmark(sql_get_metainfos, (insertion_id, ))
        if len(results) < 1:
            return
        packets = results[0][1]
        if len(results) == packets:
            print("can announce {0}".format(insertion_id))
            sql_update_metainfos = """
            SELECT DISTINCT
                insertion_id
            FROM
                announcements
            WHERE
                insertion_id = ? AND
                is_announced = 0
            """
            results = self.__execute_sql_qmark(sql_update_metainfos, (insertion_id, ))
            if len(results) == 0:
                sql_add_announcement = """
                INSERT OR IGNORE INTO announcements
                    (insertion_id, is_announced)
                VALUES
                    (?, 0)
                """
                self.__execute_sql_qmark(sql_add_announcement, (insertion_id, ))
        
    def update_metainfo_packet_fetched(self, insertion_id: str, packet: int):
        sql = """
        UPDATE metainfos
        SET is_fetched = 1
        WHERE 
            insertion_id = ? AND
            packet = ?
        """
        self.__execute_sql_qmark(sql, (insertion_id, packet))
        self.__update_metainfo_file_fetched(insertion_id)

    def get_announcable_insertions(self):
        sql = """
        SELECT DISTINCT
            insertion_id
        FROM
            announcements
        WHERE
            is_announced = 0
        """
        results = self.__execute_sql(sql)
        insertion_ids = []
        for result in results:
            insertion_ids.append(result[0])
        return insertion_ids

    def annouce_insertion(self, insertion_id: str):
        sql_update_metainfos = """
        UPDATE metainfos
        SET is_fetched = 2
        WHERE 
            insertion_id = ?
        """
        self.__execute_sql_qmark(sql_update_metainfos, (insertion_id, ))
        sql_update_annoucements = """
        UPDATE announcements
        SET is_announced = 1
        WHERE 
            insertion_id = ?
        """
        self.__execute_sql_qmark(sql_update_annoucements, (insertion_id, ))

    def get_v(self, key: str):
        sql = """
        SELECT DISTINCT
            k, v
        FROM
            kvs
        WHERE
            k = ?
        """
        results = self.__execute_sql_qmark(sql, (key, ))
        if len(results) != 1:
            return
        return results[0][1]

    def put_kv(self, key: str, value: bytes):
        sql = """
        REPLACE INTO kvs
            (k, v)
        VALUES
            (?, ?)
        """
        self.__execute_sql_qmark(sql, (key, value))

    