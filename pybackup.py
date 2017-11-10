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
arguments = docopt(__doc__, version='pybackup 0.3')
print(arguments)


'''
日志配置
'''
def confLog():
    log_file=[ x for x in arguments['ARG_WITH_NO_--'] if 'logfile' in x ]
    if not log_file:
        print('You must specify the --logfile option')
        sys.exit(1)
    else:
        logging.basicConfig(level=logging.DEBUG,
            format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
            datefmt='%a, %d %b %Y %H:%M:%S',
            filename=log_file,
            filemode='a')
        arguments['ARG_WITH_NO_--'].remove(log_file[0])

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
cf.read(os.getcwd()+'/pybackup.conf')
section_name = 'CMDB1'
cm_host = cf.get(section_name, "db_host")
cm_port = cf.get(section_name, "db_port")
cm_user = cf.get(section_name, "db_user")
cm_passwd = cf.get(section_name, "db_passwd")
cm_use = cf.get(section_name, "db_use")

section_name = 'TDB'
tdb_host = cf.get(section_name, "db_host")
tdb_port = cf.get(section_name, "db_port")
tdb_user = cf.get(section_name, "db_user")
tdb_passwd = cf.get(section_name, "db_passwd")
tdb_use = cf.get(section_name, "db_use")
tdb_list = cf.get(section_name, "db_list")

'''
获取查询数据库的语句
'''
def getDBS(targetdb):
    if tdb_list:
        sql = 'select SCHEMA_NAME from schemata where 1=1 '
        dbs = tdb_list.split(',')
        for i in range(0,len(dbs)):
            if i == 0:
                sql += "and (SCHEMA_NAME like '" + dbs[i] + "'"
            elif i == len(dbs)-1:
                sql += "or SCHEMA_NAME like '" + dbs[i] + "')"
            else:
                sql += "or SCHEMA_NAME like '" + dbs[i] + "'"
        bdb = targetdb.dbs(sql)
        bdb_list = []
        for i in range(0,len(bdb)):
            bdb_list += bdb[i]
        return bdb_list
    else:
        return None


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

    def dbs(self,sql):
        self.cursor.execute(sql)
        return self.cursor.fetchall()

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
def runBackup(targetdb):
    isDatabase_arg=[ x for x in arguments['ARG_WITH_NO_--'] if 'database' in x ]
    print(isDatabase_arg)
    start_time=datetime.datetime.now()
    logging.info('Begin Backup')
    print(str(start_time) + ' Begin Backup')
    if isDatabase_arg:
        print(mydumper_args)
        cmd = getMdumperCmd(*mydumper_args)
        child = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        state = child.wait()
        logging.info(''.join(child.stdout.readlines()))
        logging.info(''.join(child.stderr.readlines()))
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
    elif not isDatabase_arg:
        print('not isDatabase_arg')
        bdb_list = getDBS(targetdb)
        print(bdb_list)
        if not bdb_list:
            logging.critical('必须指定--database或在配置文件中指定需要备份的数据库')
            sys.exit(1)
        else:
            is_complete = ''
            for i in bdb_list:
                comm = []
                comm = mydumper_args + ['--database='+ i]
                cmd = getMdumperCmd(*comm)
                child = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                state = child.wait()
                logging.info(''.join(child.stdout.readlines()))
                logging.info(''.join(child.stderr.readlines()))
                if state != 0:
                    logging.critical(i+'Backup Failed!')
                    if is_complete:
                        is_complete += ',N'
                    else:
                        is_complete += 'N'
                    end_time=datetime.datetime.now()
                    print(str(end_time) + ' ' + i + ' Backup Faild')
                else:
                    if is_complete:
                        is_complete += ',Y'
                    else:
                        is_complete += 'Y'
                    end_time=datetime.datetime.now()
                    logging.info( i+' End Backup')
                    print(str(end_time) + ' ' + i + ' Backup Complete')
        end_time=datetime.datetime.now()
        elapsed_time = (end_time - start_time).seconds
        full_comm = 'mydumper ' + ' '.join(mydumper_args) + ' database='+ ','.join(bdb_list)
        return start_time,end_time,elapsed_time,is_complete,full_comm

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
        if arguments['mydumper']:
            mydumper_args=[ '--'+x for x in arguments['ARG_WITH_NO_--'] ]

        targetdb = Fandb(tdb_host,tdb_port,tdb_user,tdb_passwd,tdb_use)
        bk_id = str(uuid.uuid1())
        bk_server = getIP()
        start_time,end_time,elapsed_time,is_complete,bk_command = runBackup(targetdb)
        safe_command = safeCommand(bk_command)
        bk_dir=[ x for x in arguments['ARG_WITH_NO_--'] if 'outputdir' in x ][0].split('=')[1]
        if is_complete:
            bk_size = getBackupSize(bk_dir)
            master_info,slave_info = getMetadata(bk_dir)
        else:
            bk_size = 'N/A'
            master_info,slave_info = 'N/A','N/A'

        CMDB=Fandb(cm_host,cm_port,cm_user,cm_passwd,cm_use)
        mydumper_version,mysql_version = getVersion(targetdb)
        sql = 'insert into user_backup(bk_id,bk_server,start_time,end_time,elapsed_time,is_complete,bk_size,bk_dir,master_status,slave_status,tool_version,server_version,bk_command) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        CMDB.insert(sql,(bk_id,bk_server,start_time,end_time,elapsed_time,is_complete,bk_size,bk_dir,master_info,slave_info,mydumper_version,mysql_version,safe_command))
        CMDB.commit()
        CMDB.close()