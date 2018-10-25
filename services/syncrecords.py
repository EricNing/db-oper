# -*- coding:utf-8 -*-

# author: zhangning

import traceback
import time

class SyncRecords(object):
    def __init__(self, mysql_sel, oracle_ins, logger, interval, retail_id, sel_prefix, ins_prefix, row_number):
        self.mysql_sel = mysql_sel
        self.oracle_ins = oracle_ins
        self.logger = logger
        self.interval = interval
        self.retail_id = retail_id
        self.sel_prefix = sel_prefix
        self.ins_prefix = ins_prefix
        self.row_number = row_number

    def sync(self):
        while True:
            try:
                # sync_ecrinfo
                sql = 'select ecrid, busiid, ecrno, ecrtype, version, ip, insertdatetime from %s.gy_ecrinfo' % self.sel_prefix
                res = self.oracle_ins.select_all(sql)
                if len(res) != 0:
                    self.mysql_sel.sync_data(res, self.ins_prefix + '.' + 'test_ecrinfo', ['ecrid'.upper()], ['busiId'.upper(), 'ecrType'.upper(), 'ip'.upper()])
            except Exception as e:
                logger_info = 'execute sql error:\n%s' % (traceback.format_exc())
                # print logger_info
                if self.logger is not None:
                    self.logger.error(logger_info)
            finally:
                time.sleep(self.interval)