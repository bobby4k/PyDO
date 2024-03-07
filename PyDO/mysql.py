"""
pymysql
    link: https://github.com/PyMySQL/PyMySQL
"""

from .base import BasePyDO  #

import pymysql, dsnparse  #
from urllib.parse import parse_qs  #


class MySQLPyDO(BasePyDO):
    # sql占位符
    _placeholder = "%s"

    attrs = dict(
        # 提交模式
        AUTO_COMMIT=dict(
            # 默认“智能commit”
            DEFAULT=False,
            # autocommit use command like CREATE TABLE ..., VACUUM, PRAGMA
            AUTOCOMMIT=True,
        ),
    )

    def __init__(self, dsn: str, timeout: int = 10) -> None:
        super().__init__(dsn)
        cursor_factory = self.getCursorFactory(self.attrs["FETCH_MODE"]["DEFAULT"])
        auto_commit = self.attrs["AUTO_COMMIT"]["DEFAULT"]

        r = dsnparse.parse(dsn)
        # juset for charset
        query_params = parse_qs(r.query)
        charset = query_params.get("charset", [None])[0]
        if charset is None:
            charset = "utf8mb4"

        self._connect = pymysql.connect(
            user=r.username,  # The first four arguments is based on DB-API 2.0 recommendation.
            password=r.password,
            host=r.host,
            database=r.paths[0],
            port=r.port,
            charset=charset,
            # sql_mode=None,
            # read_default_file=None,
            connect_timeout=timeout,
            autocommit=auto_commit,
            # server_public_key=None,
            cursorclass=cursor_factory,
        )

    # END init

    def version(self):
        cur = self.cursor()
        if "dict" != self.attrs["FETCH_MODE"]["DEFAULT"]:
            rowcount = cur.execute("SELECT VERSION() as version")
            rows = cur.getone()
            return rows["version"] if isinstance(row, dict) else row[0]

        rowcount = cur.execute("SHOW VARIABLES LIKE '%vers%'")
        rows = cur.fetchall()
        if rows is None:
            return None

        vers = {}
        for row in rows:
            if row["Variable_name"] in [
                "version",
                "version_comment",
                "version_compile_os",
            ]:
                vers[row["Variable_name"]] = row["Value"]

        return ", ".join(vers.values())

    # END version

    ##查询DQL/DML Start
    def exec(self, sql: str, parameters=None) -> int:
        # 执行一条 SQL 语句，并返回受影响的行数
        sql = self.sql_placeholder(sql)
        parameters = self.parameters_mutate(parameters)

        cur = self.cursor()
        if parameters is None:
            rowcount = cur.execute(sql)
        else:
            rowcount = cur.execute(sql, parameters)

        return self._lazycommit(rowcount)

    def table_insert_on_duplicate_update(
        self, table: str, params: dict, params_update: dict
    ):
        # 预设插入数据行数
        fields = list(params.keys())
        values = list(params.values()) + list(params_update.values())
        fields_update = []
        for key in params_update.keys():
            fields_update.append(f"{key} = %s")

        places = list(map(lambda x: self._placeholder, fields))
        sql = f"INSERT INTO {table} ({', '.join(fields)}) VALUES ({', '.join(places)}) ON DUPLICATE KEY UPDATE {', '.join(fields_update)}"

        cur = self.cursor()
        cur.execute(sql, values)

        # autocommit
        return self._lazycommit(cur.rowcount)

    ##查询DQL End

    ##事务包装 Start
    def beginTransaction(self):
        self.connect().begin()
        self.in_transaction = True

    ##事务包装 End

    ##配置 Start
    def getCursorFactory(self, mode=None):
        # TODO test
        # https://pymysql.readthedocs.io/en/latest/modules/cursors.html
        cursor_factory = None
        match mode:
            case "dict":
                cursor_factory = pymysql.cursors.DictCursor
            case "row":
                cursor_factory = pymysql.cursors.Cursor
                # cursor_factory = pymysql.cursors.SSCursor
            case "namedtuple":
                cursor_factory = pymysql.cursors.SSDictCursor

        return cursor_factory

    # END getCursorFactory

    ##配置 End


# END class
