"""
    PyDO类模板
        - Python Data Objects

    - 共六大类方法
        1 连接与游标
        2 DQL/DML
        3 table便捷查询
        4 事务包装
        5 配置选项
        6 数据类型转换/字符串转义

    - placeholders(显示SQL占位符)仅支持 %s 方式
        #qmark style:
            cur.execute("INSERT INTO lang VALUES(%s, %s)", ("C", 1972))

    - 新加入数据库支持，需注意:
        cursor:
            rowcount
            lastrowid ==> 自定义使用last_insertId
        数据返回格式 FETCH_MODE:
            cursor_factory
        sql占位符
            _placeholder

"""

from .cost import PYDO_ATTRIBUTE  #


class BasePyDO:
    # 数据库连接
    _connect = None
    ##游标
    _cursor = None
    # 数据库连接定义
    dsn = None
    # 最后一次插入的Id
    last_insertId = None
    # 是否在事务中
    in_transaction = False

    # sql占位符
    _placeholder = "%s"

    attrs = dict()

    def __init__(self, dsn: str) -> None:
        PYDO_ATTRIBUTE.update(self.attrs)
        self.attrs = PYDO_ATTRIBUTE
        self.dsn = dsn
        # for k,v in self.params.items():
        # self.setAttribute(k,v)

    # END init

    def version(self):
        pass

    def errorCode(self):
        pass

    def errorInfo(sel):
        pass

    ##查询DQL/DML Start
    def exec(self, sql: str, parameters=None) -> int:
        # 执行一条 SQL 语句，并返回受影响的行数
        sql = self.sql_placeholder(sql)
        parameters = self.parameters_mutate(parameters)

        cur = self.cursor()
        if parameters is None:
            return cur.execute(sql).rowcount
        else:
            return cur.execute(sql, parameters).rowcount

    def query(self, sql: str, parameters=None):
        """
        #执行SQL语句, 返回当前游标状态, 可调用fetch***方法拿到>>结果集

        Args:
            sql (str): _description_
            parameters (_type_, optional): _description_. Defaults to None.

        Returns:
            cursor: 返回当前游标, 可调用方法
                fetchone
                fetchall
                fetchmany
            see: https://www.psycopg.org/docs/cursor.html?highlight=fetchall#cursor.fetchmany
        """
        sql = self.sql_placeholder(sql)
        parameters = self.parameters_mutate(parameters)

        cur = self.cursor()
        return cur.execute(sql, parameters)

    def fetch_all(self, sql: str, parameters=None):
        rowcount = self.query(sql, parameters)
        return self.cursor().fetchall()

    def fetch_one(self, sql: str, parameters=None):
        rowcount = self.query(sql, parameters)
        return self.cursor().fetchone()

    def lastInsertId(self):
        cur = self.cursor()
        # 如果游标自带lastrowid则直接使用, 否则使用自定义
        return cur.lastrowid if hasattr(cur, "lastrowid") else self.last_insertId

    def _lazycommit(self, rowcount: int):
        """
        非事务中, 快捷函数 懒人版自动提交

        Returns:
            rowcount
        """
        if rowcount > 0 and not self.inTransaction():
            self.connect().commit()

        return rowcount

    # ENd _autocommit

    ##查询DQL End

    ##快捷的table操作 Start
    def table_replaces(self, table: str, rows: dict | list) -> int:
        return self.table_inserts(table, rows, use_replace=True)

    def table_inserts_ignore(self, table: str, rows: dict | list) -> int:
        return self.table_inserts(table, rows, use_ignore=True)

    def table_inserts(
        self,
        table: str,
        rows: dict | list,
        use_replace: bool = False,
        use_ignore: bool = False,
    ) -> int:
        """
        table快捷插入
            - 返回修改的行数

        Args:
            table (str): 表名
            rows (dict/list): 插入的数据
                case1 {user:1234,name:'tony'}
                case2 [
                    {user:1234,name:'matt'},
                    {user:5678,name:'peter'},
                    ]

        Returns:
            int: 返回修改的行数
        """
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
        action = "INSERT" if use_replace is False else "REPLACE"
        if use_ignore:
            action += " IGNORE"
        sql = (
            f"{action} INTO {table} ({', '.join(fields)}) values ({', '.join(places)})"
        )

        cur = self.cursor()
        rows = self.parameters_mutate(rows)
        if len_rows > 1:
            cur.executemany(sql, rows)
        else:
            cur.execute(sql, rows)

        # autocommit
        return self._lazycommit(cur.rowcount)

    def table_inserts_on_duplicate_update(self, table: str, rows: dict | list) -> int:
        """插入时主键冲突即更新
         - Mariadb: ON DUPLICATE KEY UPDATE value = VALUES(value);
         - PostgreSQL: ON CONFLICT (id) DO UPDATE SET value = excluded.value;

        Args:
            table (str): _description_
            rows (dict | list): _description_

        Returns:
            int: 更新行数
        """
        # 预设插入数据行数
        len_rows = 0
        if len(rows) == 0:
            raise ValueError("empty rows")

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
        sql += f" ON DUPLICATE KEY UPDATE " + ', '.join(
            map(lambda x: f"{x} = VALUES({x})", fields)
        )

        cur = self.cursor()
        rows = self.parameters_mutate(rows)
        if len_rows > 1:
            cur.executemany(sql, rows)
        else:
            cur.execute(sql, rows)

        # autocommit
        return self._lazycommit(cur.rowcount)

    # 返回插入行主键
    def table_insert(self, table: str, row) -> int:
        res = self.table_inserts(table, row)
        return self.lastInsertId() if res == 1 else 0

    def table_select(self, table: str, params: dict = {}, orderbydesc=None, limit=1):
        """
        仅支持 key = value为条件的select语句
            不支持空条件的全表扫描

        Args:
            table (str): _description_
            params (dict): _description_

        Returns:
            limit =1: 单个结果集(dict)
            limit >1: 全部结果集(list)
        """
        sql, parameters = self._table_select_sql(table, params, orderbydesc, limit)
        return (
            self.fetch_one(sql, parameters)
            if limit == 1
            else self.fetch_all(sql, parameters)
        )

    def table_delete(self, table: str, params: dict = {}, limit: int = None):
        sql, parameters = self._table_select_sql(table, params, limit=limit)
        rowcount = self.exec(sql.replace('SELECT *', 'DELETE'), parameters)
        # autocommit
        return self._lazycommit(rowcount)

    def _table_select_sql(
        self,
        table: str,
        params: dict = {},
        orderbydesc: str | list = None,
        limit: int = None,
    ) -> (str, list):
        sql = f"SELECT * FROM {table} "
        parameters = list()
        condition = list()

        if len(params) > 0:
            # raise ValueError("not support empty condition.")
            for k, v in params.items():
                if isinstance(v, list):
                    vlist = [self.quote(x) for x in v]
                    condition.append(f"{k} IN ({','.join(vlist)})")
                else:
                    condition.append(f"{k} = {self._placeholder}")
                    parameters.append(v)

            sql += 'WHERE ' + ' AND '.join(condition)

        # TODO
        if isinstance(orderbydesc, str):
            orderbylist = self.orderby_mutate((orderbydesc, 'DESC'))
        else:
            orderbylist = self.orderby_mutate(orderbydesc)
        if len(orderbylist) > 0:
            sql += " ORDER BY " + ','.join(orderbylist)

        if limit is not None and limit not in [-1, '-1']:
            sql += f" LIMIT {limit}"

        return sql, parameters
        # END _table_select_sql

    def table_update(
        self, table: str, whereParams: dict, updateParams: dict, limit=1
    ) -> int:
        """
        仅支持带条件 且 SET k=v 的update操作
            - 注意:带limit限制

        Args:
            table (str): 表名
            whereParams (dict): 条件字段
            updateParams (dict): 更新字段
            limit (int, optional): Defaults to 1.

        Returns:
            int: 更新影响行数
        """
        if whereParams is None or len(whereParams) == 0:
            raise ValueError("not support empty condition.")

        parameters = list()
        updation = list()
        for k, v in updateParams.items():
            updation.append(f"{k} = {self._placeholder}")
            parameters.append(v)

        condition = list()
        for k, v in whereParams.items():
            condition.append(f"{k} = {self._placeholder}")
            parameters.append(v)

        sql = (
            f"UPDATE {table} SET {', '.join(updation)} WHERE {' AND '.join(condition)}"
        )
        # ? SQLite 默认不支持update/delete带limit, so 先禁用之
        # sql += f" LIMIT {int(limit)}"
        return self.exec(sql, parameters)

    ##table操作 End

    ##连接与游标 Start
    def original_connect(self):
        """
        返回最原始的数据库连接, 不会发生重连

        Returns:
            _type_: 数据库连接 sqlite3/psycopy2 .connect
        """
        return self._connect

    def connect(self):
        return self._connect

    def cursor(self):
        if not self._cursor:
            self._cursor = self.connect().cursor()
            # self._cursor = self._connect.cursor()

        return self._cursor

    def cursor_close(self):
        if self._cursor:
            self._cursor.close()
            self._cursor = None

    def __delete__(self):
        if self._cursor:
            self._cursor.close()
        self._connect.close()

    # END delete
    ##连接/游标 End

    ##事务包装 Start
    def beginTransaction(self):
        self.connect().beginTransaction()
        self.in_transaction = True

    def commit(self):
        self.connect().commit()
        self.in_transaction = False
        self.cursor_close()

    def rollBack(self):
        self.connect().rollBack()
        self.in_transaction = False
        self.cursor_close()

    def inTransaction(self):
        return self.in_transaction

    ##事务包装 End

    ##配置 Start
    def getAvailableDrivers(self):
        """
        Return an array of available PyDO drivers
        """
        return {
            "sqlite": "sqlite3",
            "mysql": "pymysql",
            "postgres": "psycopg2",
        }

    # END avaialableDrivers

    def getAttribute(self, attribute):
        return self.attrs[attribute] if attribute in self.attrs else None

    def setAttribute(self, attribute, value) -> bool:
        val_fare = True
        if attribute == "FETCH_MODE":
            val_fare = self.setFetchMode(value)
            self.attrs[attribute]["DEFAULT"] = value
        elif attribute == "AUTO_COMMIT":
            mode = (
                self.attrs[attribute]["AUTOCOMMIT"]
                if value
                else self.attrs[attribute]["DEFAULT"]
            )
            val_fare = self.setAutoCommit(mode)
        # END if
        return val_fare

    # END  setAttibute

    def setAutoCommit(self, mode):
        # 待子类重写
        pass

    def setFetchMode(self, mode=None):
        """
        设置返回数据的格式

        Args:
            mode:
                - None: tuple
                - dict: 字典
                - row: Row
                - namedtuple: 具名元组
        """
        _connect = self.connect()
        cursor_factory = self.getCursorFactory(mode)

        if hasattr(_connect, "row_factory"):  # sqlite3
            _connect.row_factory = cursor_factory
        elif hasattr(_connect, "cursor_factory"):  # psycopg2
            _connect.cursor_factory = cursor_factory
        elif hasattr(_connect, "cursorclass"):  # pymysql
            _connect.cursorclass = cursor_factory

    # END setFetchMode

    def getCursorFactory(self, mode=None):
        ##自定义返回格式
        return None

    ##配置 End

    ##数据转义 Start
    def sql_placeholder(self, sql: str, old="%s", new=None):
        # 与pymysql和psycopg2保持一致 使用%s
        new = self._placeholder if new is None else new
        return sql.replace(old, new) if old != new else sql

    @staticmethod
    def quote(s: str):
        # Quotes a string for use in a query
        return repr(s)

    @staticmethod
    def parameters_mutate(parameters):
        # sqlite see: https://docs.python.org/3/library/sqlite3.html#sqlite3-placeholders
        if isinstance(parameters, dict):
            parameters = tuple(parameters.values())
        elif isinstance(parameters, list) and len(parameters) > 0:
            if isinstance(parameters[0], dict):
                # 第一项是dict, 默认为所有项都是dict
                parameters = list(map(lambda x: tuple(x.values()), parameters))
            elif isinstance(parameters[0], list):
                # 第一项是list, 默认为所有项都是list
                parameters = list(map(lambda x: tuple(x), parameters))
            else:
                # 默认 list转tuple
                parameters = tuple(parameters)

        return parameters
        # END parameters_mutate

    @staticmethod
    def orderby_mutate(orderbys: str | list | tuple) -> list:
        """orderby 参数组合，使用的时候:
            - sql += f" ORDER BY "+ ','.join(oderby_mutate)


        Args:
            orderbys (str | list | tuple): _description_

        Returns:
            list: _description_
        """
        vtype = type(orderbys)
        vlist = []
        if vtype == str:
            vlist = [orderbys]
        elif vtype == tuple:
            if orderbys[1].upper() not in ['DESC', 'ASC']:
                raise ValueError(f"sort key must with DESC/ASC, '{orderbys[1]}' give")
            else:
                vlist = [f"{orderbys[0]} {orderbys[1]}"]
        elif vtype == list:
            for orow in orderbys:
                vlist += self.orderby_mutate(orow)
        elif orderbys is None:
            pass
        else:
            raise ValueError(f"unsupport type orderby:{vtype}")

        return vlist
        # END orderby_mutate

    ##数据转义 End


# END class
