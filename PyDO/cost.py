##配置参数
PYDO_ATTRIBUTE = dict( 
    #返回格式
    FETCH_MODE = dict(
        DEFAULT = 'dict',
        TUPLE   = None,
        DICT    = 'dict',
        ROW     = 'row',
        NAMEDTUPLE  = 'namedtuple', #具名元组
    ),
    
    #提交模式
    AUTO_COMMIT = dict(
        DEFAULT = False,
        AUTOCOMMIT = True,
    ),
)

