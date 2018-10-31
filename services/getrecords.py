# -*- coding:utf-8 -*-

# author: zhangning

import traceback
import time

class GetRecords(object):
    def __init__(self, mysql_ins, oracle_sel, logger, interval, retail_id, table_name_sel, table_name_ins, sel_prefix, ins_prefix, row_number):
        self.mysql_ins = mysql_ins
        self.oracle_sel = oracle_sel
        self.logger = logger
        self.interval = interval
        self.retail_id = retail_id
        self.table_name_sel = table_name_sel
        self.table_name_ins = table_name_ins
        self.ins_prefix = sel_prefix
        self.sel_prefix = ins_prefix
        self.row_number = row_number

    def get_datas(self):
        while True:
            try:
                sql = "select max(pf_num_id) ID from %s.%s where retail_id=%s" % (self.ins_prefix, self.table_name_ins, self.retail_id)
                res = self.mysql_ins.select_one(sql)
                if res['ID'] is None:
                    _max_id = 0
                else:
                    _max_id = res['ID']
                sql = 'select id,city_code,retail_id,retail_name,in_date,retailer_id,retailer_name,meat_batch_id, vege_batch_id,voucher_type,goods_code,goods_name,rec_goods_name,in_time,booth_num,electronic_id,goods_photo,serial_num,settlement_num,weight,price,area_origin_id,area_origin_name,ws_supplier_id,ws_supplier_name,update_time2,err,dt,update_time,num_id pf_num_id,sale_type from %s  \
                       where num_id>%s order by num_id asc fetch first %s rows only' % (self.sel_prefix+'.'+self.table_name_sel, _max_id, self.row_number)
                res = self.oracle_sel.select_all(sql)
                _records = len(res)
                if _records != 0:
                    for i in res:
                        self.mysql_ins.insert_one(self.ins_prefix + '.' + self.table_name_ins, i)
                # print "Max id of %s.%s is %s, insert %s records into %s.%s !" % (self.ins_prefix, self.table_name_ins, _max_id, _records, self.ins_prefix, self.table_name_ins)
                self.logger.info("Max id of %s.%s is %s, insert %s records into %s.%s !" % (self.ins_prefix, self.table_name_ins, _max_id, _records, self.ins_prefix, self.table_name_ins))
            except Exception as e:
                logger_info = 'execute sql error:\n%s' % (traceback.format_exc())
                # print logger_info
                self.logger.error(logger_info)
            finally:
                time.sleep(self.interval)