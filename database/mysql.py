# -*- coding:utf-8 -*-
#
# author : zhangning

import traceback
import pymysql
from pymysql.cursors import DictCursor
# from DBUtils.PooledDB import PooledDB

class Mysql(object):
    def __init__(self, host, port, user, passwd, database, charset, logger=None):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.database = database
        self.charset = charset
        self.logger = logger
        self._conn = None
        self._cursor = None
        self.connect_db()

    def connect_db(self):
        try:
            self._conn = pymysql.connect(host=self.host, port=self.port, user=self.user, passwd=self.passwd,
                                database=self.database, use_unicode=False, charset=self.charset, cursorclass=DictCursor
                                )
            self._cursor = self._conn.cursor()
            return True

        except Exception as e:
            if self.logger is not None:
                logger_info = 'connect mysql failed\n %s' % (traceback.format_exc())
                self.logger.error(logger_info)
            return False

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
                    self.logger.info('cannot connect to mysql,try to reconnect mysql!')

                self.connect_db()
                if self.conn_ping() is True:
                    if self.logger is not None:
                        self.logger.info('reconnect mysql succeeded!')
                    break
                else:
                    if self.logger is not None:
                        self.logger.info('reconnect mysql failed!')
        else:
            pass

    #获取Select的SQL语句
    def get_select_sql(self, table_name, column_name, max_id, row_num):
        sql = 'select * from %s where %s > %s order by %s limit %s' % (table_name, column_name, max_id, column_name, row_num)
        return sql

    def select_one(self, sql):
        self.check_db_conn()
        self._cursor.execute(sql)
        res = self._cursor.fetchone()
        self.commit()
        return res

    def select_all(self, sql):
        self.check_db_conn()
        self._cursor.execute(sql)
        res = self._cursor.fetchall()
        self.commit()
        return res

    def check_record(self, table_name, where_dict):
        where_fiedls = []
        where_values = []

        for k, v in where_dict.iteritems():         #python3的items对应 python2.7的iteritems，返回一个迭代器
            where_fiedls.append(k)
            where_values.append(v)

        where_fiedls = ' and '.join(map(lambda x: '%s=%%s' % x, where_fiedls))
        sql = 'select count(1) cnt from %s where %s' % (table_name, where_fiedls)

        self._cursor.execute(sql, where_values)
        res = self._cursor.fetchone()
        self.commit()

        if res['cnt'] > 0:
            return True
        else:
            return False

    def get_insert_sql(self, table_name, value_dict):
        """
        :param table_name:
        :param value_dict:
        :type value_dict: dict
        :return:
        """
        fields = ','.join(value_dict.keys())
        val_fields = ','.join(map(lambda x: '%s', value_dict.keys()))
        sql = 'insert into %s(%s) values(%s)' % (table_name, fields, val_fields)
        return sql

    # 插入一条记录
    def isnert_one(self, sql, value):
        self.check_db_conn()
        self._cursor.execute(sql, value)

    # 提交操作
    def commit(self):
        self._conn.commit()

    # 关闭数据库连接
    def close(self):
        self._cursor.close()
        self._conn.close()