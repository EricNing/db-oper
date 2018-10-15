# -*- coding:utf-8 -*-

# author : zhangning

from database.mysql import Mysql
from database.oracle import Oracle
from utils.log import get_logger
from multiprocessing import Process
import ConfigParser
import os
from push_records.trans_records import myProcess

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

def trans_push(mysql_settings, oracle_settings, logger, tran_record_interval, retail_id, table_name_sel, table_name_ins, column_name, sel_prefix, ins_prefix, row_number):
    log = get_logger(logger)
    mysql_sel = Mysql(host=mysql_settings['host'], port=mysql_settings['port'],
                    user=mysql_settings['user'], passwd=mysql_settings['password'],
                    database=mysql_settings['database'], charset=mysql_settings['charset'],
                    logger=log
                  )
    oracle_ins = Oracle(host=oracle_settings['host'], port=oracle_settings['port'],
                    user=oracle_settings['user'], password=oracle_settings['password'],
                    sid=oracle_settings['sid'], logger=log
                  )
    trp = myProcess(mysql_sel, oracle_ins, log, tran_record_interval, retail_id, table_name_sel, table_name_ins, column_name, sel_prefix, ins_prefix, row_number)
    trp.push()


if __name__ == "__main__":
    cf = ConfigParser.ConfigParser()
    cf.read('db_config.conf')

    _tran_record_interval = int(cf.get('interval', 'tran_records'))
    _in_record_interval = int(cf.get('interval', 'in_records'))
    _retail_id = cf.get('column', 'retail_id')
    _sel_prefix = cf.get('column', 'sel_prefix')
    _ins_prefix = cf.get('column', 'ins_prefix')
    _row_number = cf.get('column', 'row_number')

    oracle_settings = dict()
    oracle_settings['host'] = cf.get('oracle_ins', 'host')
    oracle_settings['port'] = int(cf.get('oracle_ins', 'port'))
    oracle_settings['user'] = cf.get('oracle_ins', 'user')
    oracle_settings['password'] = cf.get('oracle_ins', 'password')
    oracle_settings['sid'] = cf.get('oracle_ins', 'sid')

    mysql_settings = dict()
    mysql_settings['host'] = cf.get('mysql_sel', 'host')
    mysql_settings['port'] = int(cf.get('mysql_sel', 'port'))
    mysql_settings['user'] = cf.get('mysql_sel', 'user')
    mysql_settings['password'] = cf.get('mysql_sel', 'password')
    mysql_settings['database'] = cf.get('mysql_sel', 'database')
    mysql_settings['charset'] = cf.get('mysql_sel', 'charset')

    logger = cf.get('logfile', 'logfile')

    p_tranrecords = Process(target=trans_push, args=(mysql_settings, oracle_settings, logger, _tran_record_interval, _retail_id, 'gy_retail_market_tran_summ', 'gy_retail_market_tran_summ', 'pf_num_id', _sel_prefix, _ins_prefix, _row_number), name='tran_Process')
    #p_payinforecords = Process(target=trans_push, args=(mysql_settings, oracle_settings, logger, _in_record_interval, _retail_id, 'payinfopos', 'gy_payinfopos', 'id', _sel_prefix, _ins_prefix, _row_number), name='payinfo_Process')

    p_tranrecords.start()
    #p_payinforecords.start()

    p_tranrecords.join()
    #p_payinforecords.join()