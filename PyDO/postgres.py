"""
Psycopg2 对象
    link: https://www.psycopg.org/docs/usage.html

    - #TODO:
        - 1 连接池
            https://www.psycopg.org/docs/pool.html
        - 2 JSON数据
            要操作PostgreSQL数据库中的JSON数据类型，您需要在psycopg2中使用以下函数之一：
            psycopg2.extras.Json
            psycopg2.extras.Jsonb
"""
from .base import BasePyDO  #

import psycopg2  #
from psycopg2 import extras  #
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, ISOLATION_LEVEL_DEFAULT  #


class PostgresPyDO(BasePyDO):
    # sql占位符
    _placeholder = "%s"

    attrs = dict(
        # 提交模式
        AUTO_COMMIT=dict(
            # 默认“智能commit”
            DEFAULT=ISOLATION_LEVEL_DEFAULT,
            # autocommit use command like CREATE TABLE ..., VACUUM, PRAGMA
            AUTOCOMMIT=ISOLATION_LEVEL_AUTOCOMMIT,
        ),
    )

    def __init__(self, dsn: str) -> None:
        super().__init__(dsn)
        ##返回格式
        cursor_factory = self.getCursorFactory(self.attrs["FETCH_MODE"]["DICT"])
        self._connect = psycopg2.connect(dsn=dsn, cursor_factory=cursor_factory)

    # END init

    def version(self):
        cur = self.cursor()
        cur.execute("SELECT VERSION() as version")
        row = cur.fetchone()
        version = row["version"] if isinstance(row, dict) else row[0]
        return version

    # END version

    ##查询DQL/DML Start
    def exec(self, sql: str, parameters=None) -> int:
        return self._lazycommit(super().exec(sql, parameters))

    ##查询DQL/DML End

    ##快捷的table操作 Start

    def table_insert(self, table: str, row, returning: list = []) -> int:
        return self.table_inserts(table, row, returning)

    def table_inserts(self, table: str, rows: dict, returning: list = []) -> any:
        # 预设插入数据行数
        len_rows = 0

        if isinstance(rows, dict):
            fields = list(rows.keys())
            len_rows = 1
        elif isinstance(rows, list):
            fields = list(rows[0].keys())
            len_rows = len(fields)
        else:
            raise ValueError(f"param:rows only suport dict and list")

        places = list(map(lambda x: self._placeholder, fields))
        sql = f"INSERT INTO {table} ({', '.join(fields)}) values ({', '.join(places)})"

        # sql add returning
        use_return = False
        if len(returning) > 0:
            use_return = True
            sql = f"{sql} RETURNING {', '.join(returning)}"

        # exec
        cur = self.cursor()
        rows = self.parameters_mutate(rows)
        if len_rows > 1:
            cur.executemany(sql, rows)
        else:
            cur.execute(sql, rows)

        # lazycommit
        self._lazycommit(cur.rowcount)

        if cur.rowcount == 0 or use_return is False:
            return 0

        # returning
        return cur.fetchall() if len_rows > 1 else cur.fetchone()

    ##快捷的table操作 End

    ##配置 Start
    def getCursorFactory(self, mode=None):
        ##see: https://www.psycopg.org/docs/extras.html
        cursor_factory = None
        match mode:
            case "dict":
                cursor_factory = psycopg2.extras.RealDictCursor
            case "row":
                cursor_factory = psycopg2.extras.DictCursor
            case "namedtuple":
                cursor_factory = psycopg2.extras.NamedTupleCursor

        return cursor_factory

    # END getCursorFactory

    def setAutoCommit(self, mode):
        self.connect().set_isolation_level(mode)

    ##配置 End


# END class
