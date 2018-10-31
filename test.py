# -*- coding:utf-8 -*-
from database.oracle import Oracle
from database.mysql import Mysql
from services.syncrecords import SyncRecords


import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

# select id, city_code, retail_id, retail_name, in_date, retailer_id, retailer_name, meat_batch_id, vege_batch_id, voucher_type, goods_code, goods_name, rec_goods_name, in_time, booth_num, electronic_id, goods_photo, serial_num, settlement_num, weight, price, area_origin_id, area_origin_name, ws_supplier_id, ws_supplier_name, update_time2, err, dt, update_time, num_id, sale_type, veg_id, upload_time from gy_retail_market_in_info
if __name__ == "__main__":
    # oracle = Oracle(host='*******', port=1521, user='book', password='book', sid='orcl')
    oracle = Oracle(host='*******', port=1521, user='cc330212333', password='peb2d2e131c0d', sid='*******')
    # mysql = Mysql(host='*******', port=3306, user='vgt', passwd='*******', database='vegetable', charset='utf8')
    # mysql = Mysql(host='*******', port=3306, user='vgt', passwd='*******', database='vegetable', charset='utf8')
    mysql = Mysql(host='127.0.0.1', port=13306, user='vgt', passwd='*******', database='vegetable', charset='utf8')

    sql = 'select max(pf_num_id) ID from vegetable.gy_retail_market_in_info'
    res = mysql.select_one(sql)
    if res['ID'] is None:
        _max_id = 0
    else:
        _max_id = res['ID']
    sql = 'select id, city_code, retail_id, retail_name, in_date, retailer_id, retailer_name, meat_batch_id, vege_batch_id, voucher_type, goods_code, goods_name, rec_goods_name, in_time, booth_num,electronic_id,goods_photo,serial_num,settlement_num,weight, price,area_origin_id,area_origin_name,ws_supplier_id,ws_supplier_name,update_time2,err,dt,update_time,num_id pf_num_id,sale_type from cc330212333.gy_retail_market_in_info where num_id>%s order by num_id asc fetch first 100 rows only' % (_max_id)
    # mysql._cursor.execute(sql)
    res = oracle.select_all(sql)
    print res
    _records = len(res)
    if _records != 0:
        for i in res:
            print '------------------------------'
            print i
            mysql.insert_one('vegetable.gy_retail_market_in_info', i)

    # res = oracle.select_all('select ecrid,busiid,ecrno,ecrtype,version,ip,insertdatetime from gy_ecrinfo')
    # print res
    # mysql.sync_data(res, 'test_ecrinfo', ['ecrid'.upper()], ['busiId'.upper(), 'ecrType'.upper()])

    # sql = 'select ecrid, busiid, ecrno, ecrtype, version, ip, insertdatetime from %s.gy_ecrinfo' % 'zabbix'
    # print sql

    # s1 = SyncRecords(mysql, oracle, None, 60, '330212333', 'cc330212333', 'vegetable', 100)
    # s1.sync()
    # data = {'ECRID': '90011000', 'ECRTYPE': '\xcb\xae\xb9\xfb', 'NODE_ID': '330212333', 'STATUS': 0, 'UDT': None, 'BUSIID': '3302123331027', 'SERIALNO': None, 'CLASSIFYTYPE': 0, 'ECRNO': None, 'DDT': None, 'VERSION': None, 'INSERTDATETIME': None, 'STATUSSTARTTIME': None, 'IP': '10.333.42.3', 'RETAIL_ID': '330212333', 'OPENTAG': 0}
    # sql = 'select count(1) cnt from test_ecrinfo where ecrid=%(ecrid)s'
    # print mysql.check_record(data, 'test_ecrinfo', ['ecrId'.upper()])
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

    # value_dict = [{'value': 9, 'id': 1, 'name': 'spark 2.0'}, {'value': 8, 'id': 2, 'name': 'hadoop 2.2'}]
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
    # dict1 = [{'update_time'.upper(): '20180930', 'err'.upper(): 'None', 'BIZ_ID': '3302123330001', 'record_node_id'.upper(): '330212333'}]
    # print type(dict1), len(dict1)
    #
    # sub_dict1 = [(key, dict1[0].get(key)) for key in ['BIZ_ID', 'record_node_id'.upper()]]
    # print dict(sub_dict1)
    #
    # dict2 = {'name': 'b', 'id': 2}
    # mysql.update_one(dict2, 'tmp2', ['name'], ['id'])

    # dict3 = [{'name': 'bbb', 'id': 2, 'sex': 'm'}, {'name': 'aaa', 'id': 1, 'sex': 'm'}]
    # mysql.update_many(dict3, 'tmp2', ['name'], ['id', 'sex'])

    # dict4 = [{'id': 2, 'sex': 'm'}, {'id': 1, 'sex': 'f'}]
    # mysql.delete_many('tmp2', ['id', 'sex'], dict4)

    # dict5 = {'name': 'ddd', 'id': 4, 'sex': 'm'}
    # mysql.insert_one('tmp2', dict5)
    # mysql._cursor.execute('select * from tmp2')
    # res = mysql._cursor.fetchall()
    # print res
    #
    #
    # res = mysql.check_record({'id': 4, 'name': 'ddd'}, 'tmp2', ['id'])
    # print res

    # mysql.sync_data([{'id': 4, 'name': 'ddd'}, {'id': 5, 'name': 'fff'}], 'tmp2', ['id'], None)

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
