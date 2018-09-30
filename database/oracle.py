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
        self.connect_db()

    def connect_db(self):
        try:
            self._conn = cx_Oracle.connect(self.user, self.password, "%s:%s/%s" % (self.host, self.port, self.sid))
            self._cursor = self._conn.cursor()
            return True
        except Exception as e:
            if self.logger is not None:
                logger_info = 'connect Oracle failed\n %s' % (traceback.format_exc())
                self.logger.error(logger_info)
            return False

    def make_dict(self, cursor):
        cols_name = [c[0] for c in cursor.description]
        def gendict(*args):
            return dict(zip(cols_name, args))
        return gendict

    def conn_ping(self):
        try:
            self._conn.ping()
            return True
        except:
            return False

    def check_db_conn(self):
        if self.conn_ping() is False:
            while True:
                if self.logger is not None:
                    self.logger.info('cannot connect to Oracle,try to reconnect Oracle!')

                self.connect_db()
                if self.conn_ping() is True:
                    if self.logger is not None:
                        self.logger.info('reconnect Oracle succeeded!')
                    break
                else:
                    if self.logger is not None:
                        self.logger.info('reconnect Oracle failed!')
        else:
            pass


    # requirement: oracle 12c  (fetch first n rows only)
    def get_select_sql(self, table_name, column_name, max_id, row_num):
        sql = 'select * from %s where %s > %s order by %s fetch first %s rows only' % (table_name, column_name, max_id, column_name, row_num)
        return sql

    # define cursor.rowfactory to obtain dict data instead of tuple
    def select_one(self, sql):
        self.check_db_conn()
        self._cursor.execute(sql)
        self._cursor.rowfactory = self.make_dict(self._cursor)
        res = self._cursor.fetchone()
        return res

    # define cursor.rowfactory to obtain dict data instead of list
    def select_all(self, sql):
        self.check_db_conn()
        self._cursor.execute(sql)
        self._cursor.rowfactory = self.make_dict(self._cursor)
        res = self._cursor.fetchall()
        return res

    def insert_one(self, table_name, value_dict):
        """
        :param table_name: the table name to insert records
        :param value_dict:
        :type value_dict : list, tuple or dict
        :return:

        Example:
            value_dict = {'ins_col1': val1, 'ins_col2': val2,..., 'ins_coln': valn}

            cursor.execute('insert into table_name(col1,col2,..,coln) \
                            values(:ins_col1,:ins_col2,..,:ins_coln)', value_dict)
        """

        fields = ','.join(value_dict.keys())
        val_fields = ','.join(map(lambda x: ':%s' % x, value_dict.keys()))
        sql = 'insert into %s(%s) values(%s)' % (table_name, fields, val_fields)
        self.check_db_conn()
        self._cursor.execute(sql, value_dict)
        self.commit()
        # cursor_pre = self._cursor.prepare(sql)
        # cursor_pre.execute(None, argsDict)   -- argsDict:字典类型


    # 这里用字典列表形式存放值用于executemany，实际还可以用列表存放值，插入单条记录还可以用元组存放值
    # [(val_11,val_21,...val_n1),(val_12,val_22,...val_n2),...]
    def insert_many(self, table_name, value_dict):
        """
        :param table_name : the table name to insert records
        :param value_dict : 
        :type value_dict : list of dict

        Example:
            value_dict = [{'ins_col1': val_11, 'ins_col2': val_21,..., 'ins_coln': val_n1},\
                          {'ins_col_12': val1, 'ins_col2': val_22,..., 'ins_coln': val_n2}]

            cursor.execute('insert into table_name(col1,col2,..,coln) \
                                        values(:ins_col1,:ins_col2,..,:ins_coln)', value_dict)
        """
        fields = ','.join(value_dict[0].keys())
        val_fields = ','.join(map(lambda x: ':%s' % x, value_dict[0].keys()))
        sql = 'insert into %s(%s) values(%s)' % (table_name, fields, val_fields)
        self.check_db_conn()
        self._cursor.executemany(sql, value_dict)
        self.commit()

    def update(self, value_dict, table_name, set_fields, where_fields=None):
        """
        Param Type
            value : sequences(list) of mapping(dict)
            set_fields : list     ['name',...]
            where_fields : list   ['id']
            table_name : str

        Example:
            value =  [{'set_name': 'Python v2.0', 'is_deleted': 1, 'where_id': 2}, \
                      {'is_deleted': 1, 'set_name': 'Django Book v2.0', 'where_id': 1}, \
                      {'is_deleted': 1, 'set_name': 'ZooKeeper v2.0', 'where_id': 6}]
    
            sql = 'update table_name set name=:set_name, is_deleted=:is_deleted where id=:where_id'
            oracle._cursor.executemany(sql, value)
        """
        set_sql = ','.join(map(lambda x: '%s=:%s' % (x, x), set_fields))

        if where_fields is None:
            sql = 'update %s set %s' % (table_name, set_sql)
        else:
            where_sql = ','.join(map(lambda x: '%s=:%s' % (x, x), where_fields))
            sql = 'update %s set %s where %s' % (table_name, set_sql, where_sql)
        self.check_db_conn()
        self._cursor.executemany(sql, value_dict)
        self.commit()

    def delete(self, table_name, where_fields=None, value_dict=None):
        """
        :param table_name:
        :param where_fields:
        :param value_dict:

        :type where_fields: list, ['col_1', ...,'col_n']
        :type value_dict: dict, {col1: val1, ..., coln: valn}
        :return:
        """
        if where_fields is None and value_dict is None:
            sql = 'delete from %s' % table_name
            self._cursor.execute(sql)
        else:
            where_fields = ' and '.join(map(lambda x: '%s=:%s' % (x, x), where_fields))
            sql = 'delete from %s where %s' % (table_name, where_fields)
            self._cursor.execute(sql, value_dict)
        self.commit()

    def commit(self):
        self._conn.commit()

    # close the db connection
    def close(self):
        self._cursor.close()
        self._conn.close()