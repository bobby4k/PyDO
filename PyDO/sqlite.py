"""
SQLite3 对象
    link: https://docs.python.org/3/library/sqlite3.html

    - placeholders(显示SQL占位符)仅支持 %s 方式
        与pymysql和psycopg2保持一致 使用%s
            SQL = "SELECT * FROM users WHERE id = %s"
        SQLite默认支持:  
            #qmark style:
                cur.execute("INSERT INTO lang VALUES(?, ?)", ("C", 1972))
            #named style:
                cur.execute("SELECT * FROM lang WHERE first_appeared = :year", {"year": 1972})

    - #TODO: JSON数据类型
        - 为了操作 JSON 数据类型，你需要安装 sqlite-json1 模块。你可以使用 pip install sqlite-json1 来安装这个模块
            # Query the database
            cursor.execute('SELECT json_extract(data, "$.name") FROM mytable')
"""

from .base import BasePyDO  #

import sqlite3  #


class SqlitePyDO(BasePyDO):
    # sql占位符
    _placeholder = "?"

    attrs = dict(
        # 提交模式
        AUTO_COMMIT=dict(
            # 默认“智能commit”
            DEFAULT="",
            # autocommit use command like CREATE TABLE ..., VACUUM, PRAGMA
            AUTOCOMMIT=None,
        ),
    )

    def __init__(self, dsn: str, timeout: float = 5.0) -> None:
        super().__init__(dsn)

        file = dsn.replace("sqlite:///", "")
        self._connect = sqlite3.connect(file, timeout=timeout)
        ##设置dict返回格式
        self.setAttribute("FETCH_MODE", self.attrs["FETCH_MODE"]["DICT"])

    # END init

    def version(self):
        cur = self.cursor()
        cur.execute("SELECT SQLITE_VERSION() as version")
        row = cur.fetchone()
        version = row["version"] if isinstance(row, dict) else row[0]
        return f"SQLite {version} (pysqlite2-{sqlite3.version})"

    ##查询DQL/DML Start
    def exec(self, sql: str, parameters=None) -> int:
        return self._lazycommit(super().exec(sql, parameters))

    def fetch_all(self, sql: str, parameters=None):
        return self.query(sql, parameters).fetchall()

    def fetch_one(self, sql: str, parameters=None):
        return self.query(sql, parameters).fetchone()

    ##查询DQL/DML End

    ##快捷的table操作 Start
    def table_inserts(self, table: str, rows: dict) -> int:
        return self._lazycommit(super().table_inserts(table, rows))

    ##快捷的table操作 End

    # @staticmethod
    # def quote(s:str, errors="strict"):
    #     """
    #     see: https://gist.github.com/jeremyBanks/1083518/

    #     Args:
    #         s (str): _description_
    #         errors:
    #             'strict': raise an exception in case of an encoding error
    #             'replace': replace malformed data with a suitable replacement marker, such as '?' or '\ufffd'
    #             'ignore': ignore malformed data and continue without further notice
    #             'xmlcharrefreplace': replace with the appropriate XML character reference (for encoding only)
    #             'backslashreplace': replace with backslashed escape sequences (for encoding only) This doesn't check for reserved identifiers, so if you try to create a new SQLITE_MASTER table it won't stop you.

    #     Returns:
    #         _type_: _description_
    #     """
    #     import codecs#
    #     encodable = s.encode("utf-8", errors).decode("utf-8")

    #     nul_index = encodable.find("\x00")

    #     if nul_index >= 0:
    #         error = UnicodeEncodeError("utf-8", encodable, nul_index, nul_index + 1, "NUL not allowed")
    #         error_handler = codecs.lookup_error(errors)
    #         replacement, _ = error_handler(error)
    #         encodable = encodable.replace("\x00", replacement)

    #     return "\"" + encodable.replace("\"", "\"\"") + "\""
    # #END quote

    ##事务包装 Start
    def beginTransaction(self):
        cursor = self.cursor()
        cursor.execute("BEGIN TRANSACTION")
        self.in_transaction = True

    def rollBack(self):
        cursor = self.cursor()
        cursor.execute("ROLLBACK")
        self.in_transaction = False
        self.cursor_close()

    ##事务 End

    ##配置 Start
    def getCursorFactory(self, mode=None):
        cursor_factory = None
        match mode:
            case "dict":
                cursor_factory = dict_factory
            case "namedtuple":
                cursor_factory = namedtuple_factory
            case "row":
                cursor_factory = sqlite3.Row

        return cursor_factory

    # END  getCursorFactory

    def setAutoCommit(self, mode):
        self.connect().isolation_level = mode

    ##配置 End


# END class


def dict_factory(cursor, row):
    # see: https://docs.python.org/3/library/sqlite3.html#sqlite3-howto-row-factory
    fields = [column[0] for column in cursor.description]
    return {key: value for key, value in zip(fields, row)}


def namedtuple_factory(cursor, row):
    from collections import namedtuple  #

    fields = [column[0] for column in cursor.description]
    cls = namedtuple("Row", fields)
    return cls._make(row)
