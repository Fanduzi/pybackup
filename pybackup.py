#!/usr/bin/python
# -*- coding: utf8 -*-
"""
Usage:
        pybackup.py mydumper ARG_WITH_NO_--...
        pybackup.py -h | --help
        pybackup.py --version

Options:
        -h --help                      Show help information.
        --version                      Show version.
        --config=<config_file>         Config file.
        --dbname=<database_name>       Section name in config file.

说明:
./pybackup.py mydumper 代表使用mydumper备份,你可以像使用mydumper一样传递参数,只不过在原本的mydumper命令前加上./pybackup,并且需要注意的一点是只支持长选项,并且不带'--'
例如:
./pybackup.py mydumper password=fanboshi database=test outputdir=/data4/recover/pybackup/2017-11-08 logfile=/data4/recover/pybackup/bak.log verbose=3
如果使用命令行指定参数,则必须指定logfile参数



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

if arguments['mydumper']:
    mydumper_args=[ '--'+x for x in arguments['ARG_WITH_NO_--'] ]

'''
日志配置
'''
def confLog():
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

    def version(self):
        self.cursor.execute('select version()')
        return self.cursor.fetchone()

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
    logging.info('Begin Backup')
    print(str(start_time) + ' Begin Backup')
    state=subprocess.call(cmd,shell=True)
    if state != 0:
        logging.critical('Backup Failed!')
        is_complete = 'N'
        end_time=datetime.datetime.now()
        print(str(end_time) + ' Backup Faild')
    else:
        end_time=datetime.datetime.now()
        logging.info('End Backup')
        is_complete = 'Y'
        print(str(end_time) + ' Backup Complete')
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

'''
从metadata中获取 SHOW MASTER STATUS / SHOW SLAVE STATUS 信息
'''
def getMetadata(outputdir):
    metadata = outputdir + '/metadata'
    with open(metadata,'r') as file:
        content = file.readlines()
    
    separate_pos = content.index('\n')
    
    master_status = content[:separate_pos]
    master_log=[ x.split(':')[1].strip() for x in master_status if 'Log' in x ]
    master_pos=[ x.split(':')[1].strip() for x in master_status if 'Pos' in x ]
    master_GTID=[ x.split(':')[1].strip() for x in master_status if 'GTID' in x ]
    master_info = ','.join(master_log + master_pos + master_GTID)

    slave_status = content[separate_pos+1:]
    if not 'Finished' in slave_status[0]:
        slave_log=[ x.split(':')[1].strip() for x in slave_status if 'Log' in x ][0]
        slave_pos=[ x.split(':')[1].strip() for x in slave_status if 'Pos' in x ][0]
        slave_GTID=[ x.split(':')[1].strip() for x in slave_status if 'GTID' in x ][0]
        slave_info = ','.join(slave_log + slave_pos + slave_GTID)
        return master_info,slave_info
    else:
        return master_info,'Not a slave'

'''
移除bk_command中的密码
'''
def safeCommand(cmd):
    cmd_list = cmd.split(' ')
    passwd = [ x.split('=')[1] for x in cmd_list if 'password' in x ][0]
    safe_command = cmd.replace(passwd,'supersecrect')
    return safe_command

'''
获取mydumper 版本和 mysql版本
'''
def getVersion(db):
    child=subprocess.Popen('mydumper --version',shell=True,stdout=subprocess.PIPE)
    child.wait()
    mydumper_version = child.communicate()[0].strip()
    mysql_version = db.version()
    return mydumper_version,mysql_version

if __name__ == '__main__':
    if arguments['mydumper'] and ('help' in arguments['ARG_WITH_NO_--'][0]):
        subprocess.call('mydumper --help',shell=True)
    else:
        confLog()
        bk_id = str(uuid.uuid1())
        bk_server = getIP()
        start_time,end_time,elapsed_time,is_complete,bk_command = runBackup()
        safe_command = safeCommand(bk_command)
        bk_dir=[ x for x in arguments['ARG_WITH_NO_--'] if 'outputdir' in x ][0].split('=')[1]
        if is_complete:
            bk_size = getBackupSize(bk_dir)
            master_info,slave_info = getMetadata(bk_dir)
        else:
            bk_size = 'N/A'
            master_info,slave_info = 'N/A','N/A'

        CMDB=Fandb(db_host,db_port,db_user,db_passwd,db_use)
        mydumper_version,mysql_version = getVersion(CMDB)
        sql = 'insert into user_backup(bk_id,bk_server,start_time,end_time,elapsed_time,is_complete,bk_size,bk_dir,master_status,slave_status,tool_version,server_version,bk_command) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        CMDB.insert(sql,(bk_id,bk_server,start_time,end_time,elapsed_time,is_complete,bk_size,bk_dir,master_info,slave_info,mydumper_version,mysql_version,safe_command))
        CMDB.commit()
        CMDB.close()