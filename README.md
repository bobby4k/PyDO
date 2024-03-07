## PyDO
Python DataBase Objects ¶

### Support:
- 1 类似PHP PDO简单操作数据的方法, 没有orm 只有sql与dict/list类型的操作

- 2 返回格式支持:
    - None: tuple
    - dict: **字典(默认)**
    - row: Row
    - namedtuple: 具名元组

- 3 目前仅支持项目中用到的
    - sqlite3
    - mysql(pymysql)
    - postgres(psycopg2)

- 4 insert支持dict + list(tuple)格式数据

- 5 placeholders(显示SQL占位符)仅支持 %s 方式
```
#qmark style:
    cur.execute("INSERT INTO lang VALUES(%s, %s)", ("C", 1972))
```

### Usage:
```python
# https://en.wikipedia.org/wiki/Data_source_name
dsn = "sqlite:///local_sqlite_file"
dsn = "mysql://username:password@host:port/database?charset=utf8mb4"
dsn = "postgresql://username:password@host:port/database"

timeout = 3
db = PyDO(dsn=dsn, timeout=timeout)
print(db.version())
```


### 共六大类方法
1. 连接与游标
2. DQL/DML
3. table便捷查询
4. 事务包装
5. 配置选项
6. 数据类型转换/字符串转义

### 新加入数据库支持，需注意:
- cursor:
    - rowcount
    - lastrowid ==> 自定义使用last_insertId
- 数据返回格式 FETCH_MODE:
    - cursor_factory
- sql占位符
    - _placeholder

# END FILE