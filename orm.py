#!/usr/bin/env python3
# -*- coding:utf-8 -*-

'orm数据库连接'

__author__='zhangyangrong'

import aiomysql, logging
from boto.compat import StandardError
from socks import log
from orm import Model, StringField, IntegerField
logging.basicConfig(level=logging.INFO)

async def create_pool(loop,**kw):
    ' 创建全局的连接池 '
    logging.info('create database connection pool...')
    global __pool
    # kw.get()的方式直接定义，kw['']的方式需要传入相应的属性
    __pool = await aiomysql.create_pool(
        host = kw.get('host', 'localhost'),  # 主机号
        port = kw.get('port', 3306),  # 端口号
        user = kw['user'],  # 用户名
        password = kw['password'],  # 密码
        db = kw['db'],  # 数据库
        charset = kw.get('charset', 'utf8'),  # 编码格式
        autocommit = kw.get('autocommit', True),  # 自动提交
        maxsize = kw.get('maxsize', 10),  # 最大连接数量
        minsize=kw.get('minsize', 10),  # 最小连接数量
        loop = loop
    )

async def select(sql, args, size=None):
    ' 执行Select '
    log(sql, args)
    global __pool
    async with __pool.get() as conn:
        # aiomysql.DictCursor将结果作为字典返回
        async with conn.cursor(aiomysql.DictCursor) as cur:
            # 执行语句，第一个参数传入sql语句并将语句中的?替换为%s，第二个语句传入参数
            await cur.execute(sql.replace('?', '%s'), args or ())
            # 如果size有值根据值获取行数，没有值时默认为None查询所有数据
            if size:
                # 指定一次要获取的行数
                rs = await cur.fetchmany(size)
            else:
                # 返回查询结果集的所有行（查到的所有数据）
                rs = await cur.fetchall()
        logging.info('rows returned: %s' % len(rs))
        return rs

async def execute(sql, args, autocommit=True):
    ' 执行Insert, Update, Delete '
    log(sql)
    async with __pool.get() as conn:
        # 执行改变数据的语句时判断是否自动提交，not True相当于False
        if not autocommit:
            await conn.begin()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql.replace('?', '%s'), args)
                affected = cur.rowcount
            if not autocommit:
                await conn.commit()
        except BaseException as e:
            if not autocommit:
                await conn.rollback()
            raise
        return affected

#定义一个User对象
from orm import Model, StringField, IntegerField

class User(Model):
    __table__ = 'users'

    id = IntegerField(primary_key=True)
    name = StringField()

def create_args_string(num):
    ' 根据输入的数字创建参数个数，例如：输入3返回 ?, ?, ? '
    L = []
    for n in range(num):
        L.append('?')
    # join意为用指定的字符连接生成一个新字符串
    return ', '.join(L)

class Field(object):
    ' 构建属性时的父类 '
    # __init__只是用来将传入的参数初始化给对象
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default
    # 字符输出
    def __str__(self):
        return ('%s, %s:%s' % (self.__class__.__name__, self.column_type, self.name))

# 继承父类Field
class StringField(Field):
    def __init__(self, name=None, primary_key=False, default=None):
        super().__init__(name, 'varchar', primary_key, default)

class BooleanField(Field):
    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)

class IntegerField(Field):
    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'int', primary_key, default)

class FloatField(Field):
    def __init__(self, name=None, primary_key=False, default=0.0):
        # 在sql中float可以存储为4字节或8字节，而real和float近似，不同的是real存储4字节
        super().__init__(name, 'real', primary_key, default)

class TextField(Field):
    def __init__(self, name=None, default=None):
        # text比varchar存储容量更大，text不允许有默认值，定义了也不生效，比如：text(200)
        super().__init__(name, 'text', False, default)

# metaclass意为元类，是类的模板，所以必须从'type'类型派生，一般用来动态的创建类
class ModelMetaclass(type):
    ' 根据metaclass创建实例 '
    # __new__是在__init__之前被调用的特殊方法
    # __new__是用来创建对象并返回的方法
    # __new__()方法接收到的参数依次是：当前准备创建的类的对象;类的名字;类继承的父类集合;类的方法集合（通过metaclass动态创建的类都会将类中定义的属性以K,V形式传入attrs，Key为变量名，Value为值）
    def __new__(cls, name, bases, attrs):
        # 排除Model类本身:
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        # 获取table名称，如果要创建的类中定义了__table__属性，则取__table__属性的值，如果没有定义__table__属性（为None），则使用要创建类的类名
        tableName = attrs.get('__table__', None) or name
        logging.info('found model: %s (table: %s)' % (name, tableName))
        # 获取所有的Field和主键名
        mappings = dict()
        fields = []
        primaryKey = None
        # 使用items()对字典遍历，接下来的语句操作都是为了获取键值后转存至mappings，再根据键删除类中同名属性
        for k, v in attrs.items():
            # 判断类型
            if isinstance(v, Field):
                logging.info('  found mapping: %s ==> %s' % (k, v))
                mappings[k] = v
                if v.primary_key:
                    # 找到主键
                    if primaryKey:
                        raise StandardError('Duplicate primary key for field: %s' % k)
                    primaryKey = k
                else:
                    fields.append(k)
        if not primaryKey:
            raise StandardError('Primary key not found.')
        # 使用keys()以列表形式返回一个字典所有的键
        for k in mappings.keys():
            # 根据键移除指定元素，相当于从类属性中删除该Field属性，否则，容易造成运行时错误（实例的属性会遮盖类的同名属性）
            attrs.pop(k)
        # map会将field的每一个元素传入function，返回每次function返回值的新列表
        escaped_fields = list(map(lambda f:'`%s`' % f, fields))
        # 保存属性和列的映射关系
        attrs['__mappings__'] = mappings
        # 表名
        attrs['__table__'] = tableName
        # 主键属性名
        attrs['__primary_key__'] = primaryKey
        # 除主键外的属性名
        attrs['__fields__'] = fields
        # 构造默认的SELECT, INSERT, UPDATE和DELETE语句，传入的单个值使用`%s`,多个值使用%s:
        # select语句操作时还需要拼接where条件
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        # insert语句操作时会调用create_args_string根据参数的数量拼接成(?, ?, ?)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        # update语句操作时用join和map结合，先用map使每一次lambda表达式返回的值作为新列表，再使用join连接成(值1=?, 值2=?, 值3=?)
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        # delete语句操作时只根据主键删除
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)

# metaclass=ModelMetaclass指示使用ModelMetaclass来定制类，可以读取Model的子类的映射信息
# 扩展dict
# 定义基类Model
class Model(dict, metaclass=ModelMetaclass):
    ' 定制类 '
    def __init__(self, **kw):
        ' 初始化 '
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        ' 获取值，如果取不到值抛出异常 '
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        ' 根据Key,Value设置值 '
        self[key] = value

    def getValue(self, key):
        ' 根据Key获取Value '
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        ' 获取某个属性的值，如果该对象的该属性还没有赋值，就去获取它对应的列的默认值 '
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
        return value

    # @classmethod表明该方法是类方法，类方法不需要实例化类就可以被类本身调用，第一个参数必须是cls，cls表示自身类，可以来调用类的属性、类的方法、实例化对象等
    # cls调用类方法时必须加括号，例如：cls().function()
    # 不使用@classmethod也可以被类本身调用，前提是方法不传递默认self参数，例如：def function()
    @classmethod
    async def findAll(cls, where=None, args=None, **kw):
        ' 根据条件查询 '
        # 将sql装配成一个列表，用于下列的拼接操作
        sql = [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        # 将args装配成一个空列表，用于下列的拼接操作（存放limit参数）
        if args is None:
            args = []
        orderBy = kw.get('order by', None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            # limit接受一个或两个数字参数，否则抛出异常
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?, ?')
                # extend也类似于拼接，用新列表追加到原来的列表后
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
        # 调用select方法并传入拼接好的sql语句和参数，其中sql列表用空格间隔
        rs = await select(' '.join(sql), args)
        return [cls(**r) for r in rs]

    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        ' 查询数据条数 '
        # 其中_num_是列名的代替名，返回一条数据时适用，如果返回多条数据建议去掉（同时去掉返回值中的['_num_']）
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        # sql = ['select %s from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        # 因为输出的数据条数在一行显示，所以传入数值1
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        # rs[0]返回 列名:条数，例如：{'_num_': 15}
        # rs[0]['_num_']返回 {'_num_': 15}中'_num_'的数据，运行结果为15
        return rs[0]['_num_']
        # return rs[0]

    @classmethod
    async def find(cls, pk):
        ' 根据主键查询 '
        # 此处直接引用metaclass定义过的__select__语句拼接where条件语句
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    async def save(self):
        ' 新增 '
        # 使用map将每个fields属性传入getValueOrDefault方法，获取值后返回成列表
        args = list(map(self.getValueOrDefault, self.__fields__))
        # 单独将主键传入getValueOrDefault方法，获取值后拼接
        args.append(self.getValueOrDefault(self.__primary_key__))
        # 传入插入语句和参数并执行
        rows = await execute(self.__insert__, args)
        if rows == 0:
            logging.warn('failed to update by primary key: affected rows: %s' % rows)
        else:
            logging.info('succeed to update by primary key: affected rows: %s' % rows)

    async def update(self):
        ' 更新 '
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows == 0:
            logging.warn('failed to update by primary key: affected rows: %s' % rows)
        else:
            logging.info('succeed to update by primary key: affected rows: %s' % rows)

    async def remove(self):
        ' 删除 '
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows == 0:
            logging.warn('failed to update by primary key: affected rows: %s' % rows)
        else:
            logging.info('succeed to update by primary key: affected rows: %s' % rows)