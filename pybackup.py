#!/usr/bin/python
# -*- coding: utf8 -*-
"""
Usage:
        pybackup.py mydumper ARG_WITH_NO_--...
        pybackup.py -h | --help
        pybackup.py --version
        pybackup.py mydumper --config=<config_file> --dbname=<database_name>

Options:
        -h --help                      Show help information.
        --version                      Show version.
        --config=<config_file>         Config file.
        --dbname=<database_name>       Section name in config file.
"""

import os
import sys
import subprocess
import datetime
import logging
import pymysql
import uuid
import ConfigParser

from docopt import docopt


'''
参数解析
'''
arguments = docopt(__doc__, version='pybackup 0.1')
print(arguments)

{'--config': None,
 '--dbname': None,
 '--help': False,
 '--version': False,
 'ARG_WITH_NO_--': ['user=root', 'password=mysql'],
 'mydumper': True}


if arguments['mydumper']:
    mydumper_args=[ '--'+x for x in arguments['ARG_WITH_NO_--'] ]

'''
日志配置
'''
def confLog():
#    log_file=[ x for x in arguments['ARG_WITH_NO_--'] if 'logfile' in x ][0].split('=')[1]
    log_file=[ x for x in arguments['ARG_WITH_NO_--'] if 'logfile' in x ]
    if not log_file:
        print('You must specify the --logfile option')
        sys.exit(1)
    else:
        log_path=log_file[0].split('=')[1]
        pybackup_log = log_path[0:log_path.rfind('/')+1] + 'pybackup.log'
        logging.basicConfig(level=logging.DEBUG,
            format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
            datefmt='%a, %d %b %Y %H:%M:%S',
            filename=pybackup_log,
            filemode='a')

'''
拼接mydumper命令
'''
def getMdumperCmd(*args):
    cmd='mydumper '
    for i in range(0,len(args)):
        if i == len(args)-1:
            cmd+=str(args[i])
        else:
            cmd+=str(args[i])+' '
    return(cmd)

'''
解析配置文件获取CMDB连接参数
'''
cf=ConfigParser.ConfigParser()
cf.read(os.getcwd()+'/CMDB.conf')
section_name = 'CMDB1'
db_host = cf.get(section_name, "db_host")
db_port = cf.get(section_name, "db_port")
db_user = cf.get(section_name, "db_user")
db_passwd = cf.get(section_name, "db_passwd")
db_use = cf.get(section_name, "db_use")

'''
定义pymysql类
'''
class Fandb:
    def __init__(self,host,port,user,password,db,charset='utf8mb4'):
        self.host = host
        self.port = int(port)
        self.user = user
        self.password = password
        self.db = db
        self.charset = charset
        try:
            self.conn=pymysql.connect(host=self.host, port=self.port, user=self.user,password=self.password,db=self.db,charset=self.charset)
            self.cursor=self.conn.cursor()
            self.diccursor=self.conn.cursor(pymysql.cursors.DictCursor)
        except Exception, e:
            logging.error('Failed to open file', exc_info=True)

    def insert(self,sql,val=()):
        self.cursor.execute(sql,val)

    def commit(self):
        self.conn.commit()

    def close(self):
        self.cursor.close()
        self.diccursor.close()
        self.conn.close()

'''
获取备份集大小
'''
def getBackupSize(outputdir):
    cmd = 'du -sh '+ outputdir
    child=subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE)
    child.wait()
    backup_size=child.communicate()[0].strip().split('\t')[0]
    return backup_size

'''
mydumper命令行目前只支持长选项(--)
'''
def runBackup():
    cmd=getMdumperCmd(*mydumper_args)
    start_time=datetime.datetime.now()
    logging.info('-'*20)
    logging.info('Begin Backup')
    state=subprocess.call(cmd,shell=True)
    if state != 0:
        logging.critical('Backup Failed!')
        is_complete = 'N'
        end_time=datetime.datetime.now()
    else:
        end_time=datetime.datetime.now()
        logging.info('End Backup')
        is_complete = 'Y'
    elapsed_time = (end_time - start_time).seconds
    return start_time,end_time,elapsed_time,is_complete,cmd

'''
获取ip地址
'''
def getIP():
    cmd = "/sbin/ifconfig  | /bin/grep  'inet addr:' | /bin/grep -v '127.0.0.1' | /bin/grep -v '192\.168' | /bin/grep -v '10\.'|  /bin/cut -d: -f2 | /usr/bin/head -1 |  /bin/awk '{print $1}'"
    child=subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE)
    child.wait()
    ipaddress = child.communicate()[0].strip()
    return ipaddress


if __name__ == '__main__':
    if arguments['mydumper'] and ('help' in arguments['ARG_WITH_NO_--'][0]):
        subprocess.call('mydumper --help',shell=True)
    else:
        confLog()
        bk_id = str(uuid.uuid1())
        bk_server = getIP()
        start_time,end_time,elapsed_time,is_complete,bk_command = runBackup()
        bk_dir=[ x for x in arguments['ARG_WITH_NO_--'] if 'outputdir' in x ][0].split('=')[1]
        if is_complete:
            bk_size = getBackupSize(bk_dir)
        else:
            bk_size = 'N/A'

        CMDB=Fandb(db_host,db_port,db_user,db_passwd,db_use)
        sql = 'insert into user_backup(bk_id,bk_server,start_time,end_time,elapsed_time,is_complete,bk_size,bk_dir,bk_command) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        CMDB.insert(sql,(bk_id,bk_server,start_time,end_time,elapsed_time,is_complete,bk_size,bk_dir,bk_command))
        CMDB.commit()
        CMDB.close()