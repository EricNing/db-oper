from database.oracle import Oracle
from database.mysql import Mysql
oracle = Oracle(host='10.1.23.167', port=1521, user='book', password='book', sid='orcl')
# oracle1 = Oracle(host='10.1.123.160', port=1521, user='cc330212333', password='peb2d2e131c0d', sid='prod')
mysql = Mysql(host='10.1.23.167', port=3306, user='vgt', passwd='vgt201709', database='vegetable', charset='utf8')
# mysql = Mysql(host='127.0.0.1', port=13306, user='vgt', passwd='vgt201709', database='vegetable', charset='utf8')

# res = oracle1.select_all('select * from gy_business_base_info')
# oracle.insert_many('gy_business_base_info', res)
#
# res1 = oracle.select_all('select * from gy_business_base_info')
# for i in res1:
#     print i

# dict1 = {'id': 1, 'name': 'spark'}
# print dict1.items()
# mysql._cursor.executemany('update test_upd set name=%s,value=%s where id=%s',
#                       [('spark 2.0', 12, 1), ('spark 3.0', 10, 2)])
# #mysql._cursor.executemany('update test_upd set name=%(name)s,value=%(value)s where id=%(id)s', [{'value': 15, 'id': 1, 'name': 'spark 3.0'}, {'value': 16, 'id': 2, 'name': 'hadoop 2.0'}])
# mysql._conn.commit()

value_dict = [{'value': 9, 'id': 1, 'name': 'spark 2.0'}, {'value': 8, 'id': 2, 'name': 'hadoop 2.2'}]
# mysql._cursor.executemany('delete from test_upd1 where id=%(id)s', value_dict)
# mysql._conn.commit()
# set_fields = ['value', 'name']
# where_fields = ['id']
# print mysql.update(value_dict, 'test_upd', set_fields, where_fields)
#
# res = mysql.select_all('select * from test_upd')
# mysql.insert_many('test_upd1', res)
# mysql.update([{'value': 1}], 'test_upd1', ['value'])
# # mysql.delete('test_upd1', ['id', 'name'], value_dict)
# # mysql.delete('test_upd1')
# res = mysql.select_all('select * from test_upd1')
# for i in res:
#     print i



# dict1 = {'value': 15, 'id': 1, 'name': 'spark 3.0'}
# print dict1.keys()
# print map(lambda x: '%%(%s)s' % x, dict1.keys())

# mysql.sync_data('gy_business_base_info', ['BIZ_ID'], res1)
dict1 = [{'update_time'.upper(): '20180930', 'err'.upper(): 'None', 'BIZ_ID': '3302123330001', 'record_node_id'.upper(): '330212333'}]
print type(dict1),len(dict1)

sub_dict1 = [(key, dict1[0].get(key)) for key in ['BIZ_ID', 'record_node_id'.upper()]]
print dict(sub_dict1)

# print oracle1._cursor.description
# print len(res)
# print '------------------'
# t = []
# for i in res:
#     t.append(i['biz_id'.upper()])
# print ','.join(t)
#
# sql = 'select busiInfoID from busiinfo where busiInfoID not in (%s)' % ','.join(t)
# mysql._cursor.execute(sql)
# res1 = mysql._cursor.fetchall()
# print '------------------'
# print len(res1)

# print mysql.check_record('busiinfo',{'busiInfoID': '3302123330010'})

# print mysql.check_record('busiinfo', {'busiInfoID': '3302123330001'})
# ins_data = [{'name': 'Hadoop 2.0', 'is_deleted': 0}, {'name': 'SPARK 2.1', 'is_deleted': 0}]
# sql = 'insert into books_bookinfo(name,is_deleted) values(:name,:is_deleted)'
# # oracle._cursor.executemany(sql, ins_data)
# oracle._cursor.executemany(sql, [('ZooKeeper', 0)])
# oracle._commit()

# oracle._cursor.execute('create table books_02 as select * from books_bookinfo where 1=2');
# oracle._cursor.execute('truncate table books_02');
# data = [{'set_name': 'Python v2.0', 'is_deleted': 1, 'where_id': 2}, \
#           {'is_deleted': 1, 'set_name': 'Django Book v2.0', 'where_id': 1}, \
#           {'is_deleted': 1, 'set_name': 'ZooKeeper v2.0', 'where_id': 6}]
# sql = 'update books_bookinfo set name=:set_name, is_deleted=:is_deleted where id=:where_id'
# oracle._cursor.executemany(sql, data)
# # oracle._cursor.executemany(sql, [('Python', 2), ('Django Book 2.0', 1)])
# # print oracle._cursor
# oracle._commit()

# data = oracle.select_one('select * from books_bookinfo where id=1')
# oracle.insert_one('books_02', data)
#
# res = oracle.select_all('select * from books_02')
# for i in res:
#     print i
# print '----------------------------------------------------'
#
# data1 = oracle.select_one('select id from books_bookinfo where id=1')
# print data1
# # sql = 'delete from books_02 where id=:id'
# # oracle._cursor.execute(sql, data1)
# # oracle.commit()
#
# oracle.delete('books_02', ['id'], data1)

# # # print data
# # sql = 'update books_01 set name=:name where id=:id'
# # oracle._cursor.executemany(sql, data)
# # oracle.commit()
# #
# print '----------------------------------------------------'
# res = oracle.select_all('select * from books_02')
# for i in res:
#     print i
#
# oracle._update(data, 'books_01', ['name', 'is_deleted'], ['id'])
# res = oracle.select_all('select * from books_01')
# for i in res:
#     print i
# oracle1.close()
oracle.close()
mysql.close()