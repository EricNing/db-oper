# -*- coding:utf-8 -*-
#
# author : zhangning

import traceback
import pymysql
from pymysql.cursors import DictCursor
import time
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
                time.sleep(30)
        else:
            pass

    # get the sql of select
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

    # # check if the record exists or not (one records)
    # def check_record(self, table_name, where_dict):
    #     # self.check_db_conn()
    #     where_fiedls = []
    #     where_values = []
    #
    #     for k, v in where_dict.iteritems():         #python3的items对应 python2.7的iteritems，返回一个迭代器
    #         where_fiedls.append(k)
    #         where_values.append(v)
    #
    #     where_fiedls = ' and '.join(map(lambda x: '%s=%%s' % x, where_fiedls))
    #     sql = 'select count(1) cnt from %s where %s' % (table_name, where_fiedls)
    #
    #     self._cursor.execute(sql, where_values)
    #     res = self._cursor.fetchone()
    #     self.commit()
    #
    #     if res['cnt'] > 0:
    #         return True
    #     else:
    #         return False
    #
    # def sub_dict(self, keys_list, value_dict):
    #     return dict([(key, value_dict.get(key)) for key in keys_list])

    # check if the record exists or not (one records)
    def check_record(self, value_dict, table_name, where_fields):
        where_sql = ' and '.join(map(lambda x: '%s=%%(%s)s' % (x, x), where_fields))
        sql = 'select count(1) cnt from %s where %s' % (table_name, where_sql)
        print sql
        self._cursor.execute(sql, value_dict)
        res = self._cursor.fetchone()
        self.commit()

        if res['cnt'] > 0:
            return True
        else:
            return False

    def sync_data(self, value_dict_list, table_name, where_fields=None, set_fields=None):
        """
        :param table_name:
        :param where_fields:
        :param value_dict_list:
        :type where_fields: list ['col_1',...,'col_n']
        :type value_dict_list: sequence(list) of mapping(dict)
        :return:
        """
        self.check_db_conn()
        for value_dict in value_dict_list:
            is_exists = self.check_record(value_dict, table_name, where_fields)
            print is_exists
            if is_exists is True:
                """update records"""
                self.update_one(value_dict, table_name, set_fields, where_fields)
                pass
            else:
                """insert records"""
                self.insert_one(table_name, value_dict)

    # insert one records
    def insert_one(self, table_name, value_dict):
        """
        :param table_name:
        :param value_dict:
        :type value_dict: dict
        :return:
        """
        self.check_db_conn()
        fields = ','.join(value_dict.keys())
        # val_fields = ','.join(map(lambda x: '%s', value_dict.keys()))
        val_fields = ','.join(map(lambda x: '%%(%s)s' % x, value_dict.keys()))
        sql = 'insert into %s(%s) values(%s)' % (table_name, fields, val_fields)
        print sql
        self._cursor.execute(sql, value_dict)
        self.commit()

    # insert one records
    def insert_many(self, table_name, value_dict):
        """
        :param table_name:
        :param value_dict:
        :type value_dict: sequences(list) of mapping(dict)
        :return:
        """
        self.check_db_conn()
        fields = ','.join(value_dict[0].keys())
        # val_fields = ','.join(map(lambda x: '%s', value_dict.keys()))
        val_fields = ','.join(map(lambda x: '%%(%s)s' % x, value_dict[0].keys()))
        sql = 'insert into %s(%s) values(%s)' % (table_name, fields, val_fields)
        self._cursor.executemany(sql, value_dict)
        self.commit()

    def update_one(self, value_dict, table_name, set_fields, where_fields=None):
        """
        Param Type
            value : mapping(dict)
            set_fields : list     ['name',...]
            where_fields : list   ['id']
            table_name : str

        Example:
            value =  {'set_name': 'Python v2.0', 'is_deleted': 1, 'where_id': 2}

            sql = 'update table_name set name=%(set_name)s, is_deleted=%(is_deleted)s where id=%(where_id)s'
            oracle._cursor.execute(sql, value)
        """
        set_sql = ','.join(map(lambda x: '%s=%%(%s)s' % (x, x), set_fields))

        if where_fields is None:
            sql = 'update %s set %s' % (table_name, set_sql)
        else:
            where_sql = ' and '.join(map(lambda x: '%s=%%(%s)s' % (x, x), where_fields))
            sql = 'update %s set %s where %s' % (table_name, set_sql, where_sql)
        self.check_db_conn()
        self._cursor.execute(sql, value_dict)
        self.commit()

    def update_many(self, value_dict, table_name, set_fields, where_fields=None):
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

            sql = 'update table_name set name=%(set_name)s, is_deleted=%(is_deleted)s where id=%(where_id)s'
            oracle._cursor.executemany(sql, value)
        """
        set_sql = ','.join(map(lambda x: '%s=%%(%s)s' % (x, x), set_fields))

        if where_fields is None:
            sql = 'update %s set %s' % (table_name, set_sql)
        else:
            where_sql = ' and '.join(map(lambda x: '%s=%%(%s)s' % (x, x), where_fields))
            sql = 'update %s set %s where %s' % (table_name, set_sql, where_sql)

        self.check_db_conn()
        self._cursor.executemany(sql, value_dict)
        self.commit()

    def delete_one(self, table_name, where_fields=None, value_dict=None):
        """
        :param table_name:
        :param where_fields:
        :param value_dict:
        :type where_fields: list, ['col_1', ...,'col_n']
        :type value_dict: mapping(dict),
                        {col1: val1, ..., coln: valn}
        :return:
        """
        if where_fields is None and value_dict is None:
            sql = 'delete from %s' % table_name
            self._cursor.execute(sql)
        else:
            where_fields = ' and '.join(map(lambda x: '%s=%%(%s)s' % (x, x), where_fields))
            sql = 'delete from %s where %s' % (table_name, where_fields)
            self._cursor.execute(sql, value_dict)
        self.commit()

    def delete_many(self, table_name, where_fields=None, value_dict=None):
        """
        :param table_name:
        :param where_fields:
        :param value_dict:
        :type where_fields: list, ['col_1', ...,'col_n']
        :type value_dict: sequences(list) of mapping(dict), 
                            [{col1: val1, ..., coln: valn}, ...]
        :return:
        """
        if where_fields is None and value_dict is None:
            sql = 'delete from %s' % table_name
            self._cursor.execute(sql)
        else:
            where_fields = ' and '.join(map(lambda x: '%s=%%(%s)s' % (x, x), where_fields))
            sql = 'delete from %s where %s' % (table_name, where_fields)
            self._cursor.executemany(sql, value_dict)
        self.commit()

    # commit
    def commit(self):
        self._conn.commit()

    # close the db connection
    def close(self):
        self._cursor.close()
        self._conn.close()