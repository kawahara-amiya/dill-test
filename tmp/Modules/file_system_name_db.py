import sqlite3
import os
import sys


class Target(object):

    def __init__(self, name: str, file_sys_name: str = ""):
        self.name = name
        self.file_sys_name = file_sys_name


class FileSystemNameDb(object):

    def __init__(self, dbfile_path: str):

        self.db_path = dbfile_path

        if os.path.exists(self.db_path):
            return
        with sqlite3.connect(self.db_path) as conn:
            # 本番環境ではここでDBファイルを作成することはないはず
            cursor = conn.cursor()
            cursor.execute(
                '''CREATE TABLE T_FILE_SYSTEM_NAME
                    (OriginalName text, Name text NOT NULL,
                    PRIMARY KEY(OriginalName COLLATE NOCASE),
                    UNIQUE(Name COLLATE NOCASE))''')
            cursor.execute(
                'CREATE INDEX fsnameindex ON T_FILE_SYSTEM_NAME(Name)')
            conn.commit()

    def __enter__(self):
        self.conn = sqlite3.connect(self.db_path)
        return self

    def __exit__(self, ex_type, ex_value, trace):
        self.conn.close()

    def commit(self):
        self.conn.commit()

    def store(self, target: Target):
        # テスト用
        cursor = self.conn.cursor()
        cursor.execute('INSERT OR REPLACE INTO T_FILE_SYSTEM_NAME (OriginalName,Name) VALUES (?,?)',
                       (target.name, target.file_sys_name))

    def contains_file_sys_name(self, file_sys_name: str) -> bool:
        cursor = self.conn.cursor()
        cursor.execute(
            'SELECT * FROM T_FILE_SYSTEM_NAME WHERE Name COLLATE NOCASE = ?', (file_sys_name,))
        return len(cursor.fetchall()) > 0

    def contains(self, target_name: str) -> bool:
        cursor = self.conn.cursor()
        cursor.execute(
            'SELECT * FROM T_FILE_SYSTEM_NAME WHERE OriginalName COLLATE NOCASE = ?', (target_name,))
        return len(cursor.fetchall()) > 0

    def find_all(self):
        cursor = self.conn.cursor()
        for target_raw in cursor.execute('SELECT * FROM T_FILE_SYSTEM_NAME'):
            yield Target(target_raw[0], target_raw[1])

    def find(self, target_name: str) -> Target:
        u"""
            Parameters
            ----------
                target_name: str
                    対象名

            Returns
            -------
                target: Target
                    対象名とそのファイルシステム上の名前(target.file_sys_name)を格納している
                    クラスのインスタンス

            Note
            ----
                もし対象名がDBに格納されていなかった場合は、そのファイルシステム上の名前を
                空文字としたTargetクラスのインスタンスを返している。しかし、C#から呼ばれた際には、
                CSVファイルの作成時点で必要となる対象名が必ず登録されているため、上記のような
                空文字のインスタンスが返ることはない。
        """
        if not self.contains(target_name):
            return Target(target_name, "")

        cursor = self.conn.cursor()
        cursor.execute(
            'SELECT * FROM T_FILE_SYSTEM_NAME WHERE OriginalName COLLATE NOCASE = ?', (target_name,))
        target_raw = cursor.fetchone()
        return Target(target_raw[0], target_raw[1])

    def find_with_file_sys_name(self, file_sys_name: str) -> Target:
        u"""
            Note
            ----
                返り値に関する注意はfindメソッドを参照。
        """
        if not self.contains_file_sys_name(file_sys_name):
            return Target("", file_sys_name)

        cursor = self.conn.cursor()
        cursor.execute(
            'SELECT * FROM T_FILE_SYSTEM_NAME WHERE Name COLLATE NOCASE = ?', (file_sys_name,))
        target_raw = cursor.fetchone()
        return Target(target_raw[0], target_raw[1])


if __name__ == "__main__":

    args = sys.argv
    with FileSystemNameDb(args[1]) as db:
        print(db.contains(args[2]))
