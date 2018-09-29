# -*- coding:utf-8 -*-
#
# author : zhangning

import pymysql
from pymysql.cursors import DictCursor
# from DBUtils.PooledDB import PooledDB
import traceback

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
        self._connectDB()

    def _connectDB(self):
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
                    self.logger.info('cannot connect to mysql,try to reconnect mysql!')

                self._connectDB()
                if self._connPing() == True:
                    if self.logger is not None:
                        self.logger.info('reconnect mysql succeeded!')
                    break
                else:
                    if self.logger is not None:
                        self.logger.info('reconnect mysql failed!')
        else:
            pass

    #获取Select的SQL语句
    def _getSelectSQL(self, table_name, column_name, max_id, row_num):
        sql = 'select * from %s where %s > %s order by %s limit %s' % (table_name, column_name, max_id, column_name, row_num)
        return sql

    def _selectOne(self, sql):
        self._check_db_conn()
        self._cursor.execute(sql)
        res = self._cursor.fetchone()
        self._commit()
        return res

    def _selectALL(self, sql):
        self._check_db_conn()
        self._cursor.execute(sql)
        res = self._cursor.fetchall()
        self._commit()
        return res

    def get_sql(self, table_name, fields=None, condition_fields=None):
        values = list()
        if fields is not None:
            fields = ','.join(fields)
        else:
            fields = '*'

        if condition_fields is not None:
            where_list = []
            for k, v in condition_fields.iteritems():
                item = "%s=%s" % (k, '%s')
                where_list.append(item)
                values.append(v)

            where_condition = ' and '.join(where_list)
            return 'select %s from %s where %s' % (fields, table_name, where_condition), values
        else:
            return 'select %s from %s' % (fields, table_name), values

    def _checkRecord(self, table_name, where_dict):
        # for k, v in where_dict:
            
        where_fiedls = ' and '.join(map(lambda x: '%s=%%s' % x, value_dict.keys()))   
        sql = 'select count(1) cnt from %s where %s' % (table_name, where_fiedls)
        print sql
        self._cursor.execute(sql, value_dict)
        res = self._cursor.fetchone()
        self._commit()
        return res


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