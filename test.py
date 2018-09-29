from database.oracle import Oracle
from database.mysql import Mysql
# oracle = Oracle(host='10.1.23.167', port=1521, user='book', password='book', sid='orcl')
# oracle1 = Oracle(host='10.1.123.160', port=1521, user='cc330212333', password='peb2d2e131c0d', sid='prod')
# mysql = Mysql(host='10.1.23.167', port=3306, user='vgt', passwd='vgt201709', database='vegetable', charset='utf8')
mysql = Mysql(host='127.0.0.1', port=13306, user='vgt', passwd='vgt201709', database='vegetable', charset='utf8')

# res = oracle1.select_all('select * from gy_business_base_info')
# # print oracle1._cursor.description
# # res = oracle1._cursor.fetchall()
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

print mysql.check_record('busiinfo', {'busiInfoID': '3302123330001'})
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
# oracle.close()
mysql.close()