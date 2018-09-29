# -*- coding:utf-8 -*-
#
# author : zhangning


import cx_Oracle
import traceback

class Oracle(object):
    def __init__(self, host, port, user, password, sid, logger=None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.sid = sid
        self.logger = logger
        self._conn = None
        self._cursor = None
        self._connectDB()

    def _connectDB(self):
        try:
            self._conn = cx_Oracle.connect(self.user, self.password, "%s:%s/%s" % (self.host, self.port, self.sid))
            self._cursor = self._conn.cursor()
            return True
        except Exception as e:
            if self.logger is not None:
                logger_info = 'connect Oracle failed\n %s' % (traceback.format_exc())
                self.logger.error(logger_info)
            return False

    def makedict(self, cursor):
        cols_name = [c[0] for c in cursor.description]
        def gendict(*args):
            return dict(zip(cols_name, args))
        return gendict

    def _connPing(self):
        try:
            self._conn.ping()
            return True
        except:
            return False

    def _check_db_conn(self):
        if self._connPing() == False:
            while True:
                if self.logger is not None:
                    self.logger.info('cannot connect to Oracle,try to reconnect Oracle!')

                self._connectDB()
                if self._connPing() == True:
                    if self.logger is not None:
                        self.logger.info('reconnect Oracle succeeded!')
                    break
                else:
                    if self.logger is not None:
                        self.logger.info('reconnect Oracle failed!')
        else:
            pass


    # 获取Select的SQL语句
    # requirement: oracle 12c  (fetch first n rows only)
    def _getSelectSQL(self, table_name, column_name, max_id, row_num):
        sql = 'select * from %s where %s > %s order by %s fetch first %s rows only' % (table_name, column_name, max_id, column_name, row_num)
        return sql

    # 获取单条记录，定义cursor.rowfactory，返回字典（原来返回元组）
    def _selectOne(self, sql):
        self._check_db_conn()
        self._cursor.execute(sql)
        self._cursor.rowfactory = self.makedict(self._cursor)
        res = self._cursor.fetchone()
        return res

    # 获取所有记录，定义cursor.rowfactory，返回字典（原来返回列表）
    def _selectALL(self, sql):
        self._check_db_conn()
        self._cursor.execute(sql)
        self._cursor.rowfactory = self.makedict(self._cursor)
        res = self._cursor.fetchall()
        return res

    # 插入一条记录
    def _insertOne(self, table_name, value_dict):
        # value : list/tuple/dict
        # Example:
        # value_dict = {'ins_col1': val1, 'ins_col2': val2,..., 'ins_coln': valn}
        #
        # cursor.execute('insert into table_name(col1,col2,..,coln) \
        #                 values(:ins_col1,:ins_col2,..,:ins_coln)', value_dict)

        fields = ','.join(value_dict.keys())
        val_fields = ','.join(map(lambda x: ':%s' % x, value_dict.keys()))
        sql = 'insert into %s(%s) values(%s)' % (table_name, fields, val_fields)
        self._check_db_conn()
        self._cursor.execute(sql, value_dict)
        self._commit()
        # cursor_pre = self._cursor.prepare(sql)
        # cursor_pre.execute(None, argsDict)   -- argsDict:字典类型

    # 插入多条记录
    # 这里用字典列表形式存放值用于executemany，实际还可以用列表存放值，插入单条记录还可以用元组存放值
    # [(val_11,val_21,...val_n1),(val_12,val_22,...val_n2),...]
    def _insertMany(self, table_name, value_dict):
        # value : list of dict
        # Example:
        # value_dict = [{'ins_col1': val_11, 'ins_col2': val_21,..., 'ins_coln': val_n1},\
        #               {'ins_col_12': val1, 'ins_col2': val_22,..., 'ins_coln': val_n2}]
        #
        # cursor.execute('insert into table_name(col1,col2,..,coln) \
        #                             values(:ins_col1,:ins_col2,..,:ins_coln)', value_dict)

        fields = ','.join(value_dict[0].keys())
        val_fields = ','.join(map(lambda x: ':%s' % x, value_dict[0].keys()))
        sql = 'insert into %s(%s) values(%s)' % (table_name, fields, val_fields)
        self._check_db_conn()
        self._cursor.executemany(sql, value_dict)
        self._commit()

    # 更新操作
    # 批量更新，传入字典列表
    def _update(self, value_dict, table_name, set_fields, where_fields=None):
        # value : list of dict
        # set_fields : list     ['name',...]
        # where_fields : list   ['id']
        # table_name : str
        #
        # Example:
        # value =  [{'set_name': 'Python v2.0', 'is_deleted': 1, 'where_id': 2}, \
        #           {'is_deleted': 1, 'set_name': 'Django Book v2.0', 'where_id': 1}, \
        #           {'is_deleted': 1, 'set_name': 'ZooKeeper v2.0', 'where_id': 6}]
        #
        # sql = 'update table_name set name=:set_name, is_deleted=:is_deleted where id=:where_id'
        # oracle._cursor.executemany(sql, value)

        set_sql = ','.join(map(lambda x: '%s=:%s' % (x, x), set_fields))
        where_sql = ','.join(map(lambda x: '%s=:%s' % (x, x), where_fields))
        sql = 'update %s set %s where %s' % (table_name, set_sql, where_sql)
        self._check_db_conn()
        self._cursor.executemany(sql, value_dict)
        self._commit()

    # 删除操作
    # 单条记录匹配删除
    def _delete(self, table_name, where_fields, value_dict):
        # value_dict : dict
        #   {col1: val1, ..., coln: valn}
        # where_fields : list
        #   ['col_1', ...,'col_n']

        where_fields = ','.join(map(lambda x: '%s=:%s' % (x, x), where_fields))
        sql = 'delete from %s where %s' % (table_name, where_fields)
        self._cursor.execute(sql, value_dict)
        self._commit()

    # 提交操作
    def _commit(self):
        self._conn.commit()

    # 关闭数据库连接
    def _close(self):
        self._cursor.close()
        self._conn.close()