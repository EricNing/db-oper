#!/bin/bash
. /home/ubuntu/.bashrc
# echo $PATH
cd /usr/local/data_push
count=`ps aux | grep tran_data.pyc | grep -v grep | wc -l`
# echo $count
if [ $count = 0 ]; then
	nohup /usr/bin/python2.7 /usr/local/data_push/tran_data.pyc &
fi
