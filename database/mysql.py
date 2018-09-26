# -*- coding:utf-8 -*-

# author : zhangning

import pymysql
from pymysql.cursors import DictCursor
# from DBUtils.PooledDB import PooledDB
import traceback

class Mysql(object):
    def __init__(self, host, port, user, passwd, database, charset, logger):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.database = database
        self.charset = charset
        self.logger = logger
        self._conn = None
        self._cursor = None
        self._connectDB()

    def _connectDB(self):
        try:
            self._conn = pymysql.connect(host=self.host, port=self.port, user=self.user, passwd=self.passwd,
                                database=self.database, use_unicode=False, charset=self.charset, cursorclass=DictCursor
                                )
            self._cursor = self._conn.cursor()
            return True
        except Exception as e:
            logger_info = 'connect mysql failed\n %s' % (traceback.format_exc())
            self.logger.error(logger_info)
            return False

    def _connPing(self):
        try:
            self._conn.ping()
            return True
        except:
            return False

    def _check_db_conn(self):
        if self._connPing() == False:
            while True:
                self.logger.info('cannot connect to mysql,try to reconnect mysql!')
                self._connectDB()
                if self._connPing() == True:
                    self.logger.info('reconnect mysql succeeded!')
                    break
                else:
                    self.logger.info('reconnect mysql failed!')
        else:
            pass

    def _getALL(self, sql):
        self._check_db_conn()
        self._cursor.execute(sql)
        res = self._cursor.fetchall()
        self._commit()
        return res

    def _getOne(self, sql):
        self._check_db_conn()
        # print self._cursor
        self._cursor.execute(sql)
        res = self._cursor.fetchone()
        self._commit()
        return res

    #获取Select的SQL语句
    def _getSelectSQL(self, table_name, column_name, max_id, row_num):
        sql = 'select * from %s where %s > %s order by %s limit %s' % (table_name, column_name, max_id, column_name, row_num)
        return sql

    # 获取insert的SQL语句
    def _getInsertSQL(self, table_name, value_dict):
    # The Type of value_dict is dictionary
        fields = ','.join(value_dict.keys())
        val_fields = ','.join(map(lambda x: '%s', value_dict.keys()))
        sql = 'insert into %s(%s) values(%s)' % (table_name, fields, val_fields)
        return sql

    # 插入一条记录
    def _insertOne(self, sql, value):
        self._check_db_conn()
        self._cursor.execute(sql, value)

    # 提交操作
    def _commit(self):
        self._conn.commit()

    # 关闭数据库连接
    def _close(self):
        self._cursor.close()
        self._conn.close()