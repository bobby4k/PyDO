def Database(dsn: str, timeout: float = 5.0):
    """
    PyDO工厂方法

    Args:
        dsn (str): data source name
            see: https://docs.sqlalchemy.org/en/20/core/engines.html

    Returns:
        PyDO: PyDO对象
    """
    driver = dsn.split(':')[0]
    pydo = None

    if driver == 'sqlite':
        from .sqlite import SqlitePyDO  #

        pydo = SqlitePyDO(dsn, timeout=timeout)

    elif driver.startswith('mysql'):
        from .mysql import MySQLPyDO  #

        pydo = MySQLPyDO(dsn, timeout=timeout)

    elif driver.startswith('postgres'):
        from .postgres import PostgresPyDO  #

        pydo = PostgresPyDO(dsn)

    else:
        raise ValueError(f"PyDO not support dialect+driver:{driver}")

    return pydo


# END Database
