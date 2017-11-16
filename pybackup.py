#!/usr/bin/python
# -*- coding: utf8 -*-
"""
Usage:
        pybackup.py mydumper ARG_WITH_NO_--... (([--no-rsync] [--no-history]) | [--only-backup])
        pybackup.py -h | --help
        pybackup.py --version

Options:
        -h --help                      Show help information.
        --version                      Show version.
        --no-rsync                     Do not use rsync.
        --no-history                   Do not record backup history information.
        --only-backup                  equal to use both --no-rsync and --no-history.

more help information in:
https://github.com/Fanduzi
"""

import os
import sys
import subprocess
import datetime
import logging
import pymysql
import uuid
import copy
import ConfigParser

from docopt import docopt



def confLog():
    '''日志配置'''
    log_file = [x for x in arguments['ARG_WITH_NO_--'] if 'logfile' in x]
    if not log_file:
        print('You must specify the --logfile option')
        sys.exit(1)
    else:
        log = log_file[0].split('=')[1]
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                            datefmt='%a, %d %b %Y %H:%M:%S',
                            filename=log,
                            filemode='a')
        arguments['ARG_WITH_NO_--'].remove(log_file[0])


def getMdumperCmd(*args):
    '''拼接mydumper命令'''
    cmd = 'mydumper '
    for i in range(0, len(args)):
        if i == len(args) - 1:
            cmd += str(args[i])
        else:
            cmd += str(args[i]) + ' '
    return(cmd)


'''
解析配置文件获取参数
'''
cf = ConfigParser.ConfigParser()
cf.read(os.getcwd() + '/pybackup.conf')
section_name = 'CMDB'
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

try:
    section_name = 'rsync'
    password_file = cf.get(section_name, "password_file")
    dest = cf.get(section_name, "dest")
    address = cf.get(section_name, "address")
    if dest[-1] != '/':
        dest += '/'
    rsync_enable = True
except NoSectionError, e:
    rsync_enable = False
    logging.warning('No rsync section, pass', exc_info=True)

section_name = 'pybackup'
tag = cf.get(section_name, "tag")


def getDBS(targetdb):
    '''获取查询数据库的语句'''
    if tdb_list:
        sql = 'select SCHEMA_NAME from schemata where 1=1 '
        if tdb_list != '%':
            dbs = tdb_list.split(',')
            for i in range(0, len(dbs)):
                if dbs[i][0] != '!':
                    if len(dbs) == 1:
                        sql += "and (SCHEMA_NAME like '" + dbs[0] + "')"
                    else:
                        if i == 0:
                            sql += "and (SCHEMA_NAME like '" + dbs[i] + "'"
                        elif i == len(dbs) - 1:
                            sql += " or SCHEMA_NAME like '" + dbs[i] + "')"
                        else:
                            sql += " or SCHEMA_NAME like '" + dbs[i] + "'"
                elif dbs[i][0] == '!':
                    if len(dbs) == 1:
                        sql += "and (SCHEMA_NAME not like '" + dbs[0][1:] + "')"
                    else:
                        if i == 0:
                            sql += "and (SCHEMA_NAME not like '" + dbs[i][1:] + "'"
                        elif i == len(dbs) - 1:
                            sql += " and SCHEMA_NAME not like '" + dbs[i][1:] + "')"
                        else:
                            sql += " and SCHEMA_NAME not like '" + dbs[i][1:] + "'"
        elif tdb_list == '%':
            dbs = ['%']
            sql = "select SCHEMA_NAME from schemata where SCHEMA_NAME like '%'"
        bdb = targetdb.dbs(sql)
        bdb_list = []
        for i in range(0, len(bdb)):
            bdb_list += bdb[i]
        return bdb_list
    else:
        return None


class Fandb:
    '''定义pymysql类'''

    def __init__(self, host, port, user, password, db, charset='utf8mb4'):
        self.host = host
        self.port = int(port)
        self.user = user
        self.password = password
        self.db = db
        self.charset = charset
        try:
            self.conn = pymysql.connect(host=self.host, port=self.port, user=self.user,
                                        password=self.password, db=self.db, charset=self.charset)
            self.cursor = self.conn.cursor()
            self.diccursor = self.conn.cursor(pymysql.cursors.DictCursor)
        except Exception, e:
            logging.error('Failed to open file', exc_info=True)

    def insert(self, sql, val=()):
        self.cursor.execute(sql, val)

    def version(self):
        self.cursor.execute('select version()')
        return self.cursor.fetchone()

    def dbs(self, sql):
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def commit(self):
        self.conn.commit()

    def close(self):
        self.cursor.close()
        self.diccursor.close()
        self.conn.close()


def getBackupSize(outputdir):
    '''获取备份集大小'''
    cmd = 'du -sh ' + outputdir
    child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    child.wait()
    backup_size = child.communicate()[0].strip().split('\t')[0]
    return backup_size


def runBackup(targetdb):
    '''执行备份'''
    # 是否指定了--database参数
    isDatabase_arg = [
        x for x in arguments['ARG_WITH_NO_--'] if 'database' in x]
    #备份的数据库 字符串
    start_time = datetime.datetime.now()
    logging.info('Begin Backup')
    print(str(start_time) + ' Begin Backup')
    # 指定了--database参数,则为备份单个数据库,即使配置文件中指定了也忽略
    if isDatabase_arg:
        print(mydumper_args)
        bdb = isDatabase_arg[0].split('=')[1]
        # 生成备份命令
        database = [ x.split('=')[1] for x in mydumper_args if 'database' in x ][0]
        outputdir_arg = [ x for x in mydumper_args if 'outputdir' in x ]
        temp_mydumper_args = copy.deepcopy(mydumper_args)
        if outputdir_arg[0][-1] != '/':
            temp_mydumper_args.remove(outputdir_arg[0])
            temp_mydumper_args.append(outputdir_arg[0]+'/'+database)
            last_outputdir = (outputdir_arg[0]+'/'+database).split('=')[1]
        else:
            temp_mydumper_args.remove(outputdir_arg[0])
            temp_mydumper_args.append(outputdir_arg[0]+database)
            last_outputdir = (outputdir_arg[0]+database).split('=')[1]
        cmd = getMdumperCmd(*temp_mydumper_args)
        child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        while child.poll() == None:
            stdout_line = child.stdout.readline().strip()
            if stdout_line:
                logging.info(stdout_line)
        logging.info(child.stdout.read().strip())
        state = child.returncode
        # 检查备份是否成功
        if state != 0:
            logging.critical('Backup Failed!')
            is_complete = 'N'
            end_time = datetime.datetime.now()
            print(str(end_time) + ' Backup Faild')
        else:
            end_time = datetime.datetime.now()
            logging.info('End Backup')
            is_complete = 'Y'
            print(str(end_time) + ' Backup Complete')
        elapsed_time = (end_time - start_time).seconds
        return start_time, end_time, elapsed_time, is_complete, cmd, bdb, last_outputdir
    # 没有指定--database参数
    elif not isDatabase_arg:
        # 获取需要备份的数据库的列表
        bdb_list = getDBS(targetdb)
        print(bdb_list)
        # 如果列表为空,报错
        if not bdb_list:
            logging.critical('必须指定--database或在配置文件中指定需要备份的数据库')
            sys.exit(1)
        else:
            bdb = ','.join(bdb_list)
            # 多个备份,每个备份都要有成功与否状态标记
            is_complete = ''
            # 在备份列表中循环
            for i in bdb_list:
                print(i)
                comm = []
                # 一次备份一个数据库,下次循环将comm置空
                outputdir_arg = [ x for x in mydumper_args if 'outputdir' in x ]
                temp_mydumper_args = copy.deepcopy(mydumper_args)
                if outputdir_arg[0][-1] != '/':
                    temp_mydumper_args.remove(outputdir_arg[0])
                    temp_mydumper_args.append(outputdir_arg[0]+'/'+i)
                    last_outputdir = (outputdir_arg[0]+'/'+i).split('=')[1]
                else:
                    temp_mydumper_args.remove(outputdir_arg[0])
                    temp_mydumper_args.append(outputdir_arg[0]+i)
                    last_outputdir = (outputdir_arg[0]+i).split('=')[1]
                comm = temp_mydumper_args + ['--database=' + i]
                # 生成备份命令
                cmd = getMdumperCmd(*comm)
                child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                while child.poll() == None:
                    stdout_line = child.stdout.readline().strip()
                    if stdout_line:
                        logging.info(stdout_line)
                logging.info(child.stdout.read().strip())
                state = child.returncode
                if state != 0:
                    logging.critical(i + 'Backup Failed!')
                    # Y,N,Y,Y
                    if is_complete:
                        is_complete += ',N'
                    else:
                        is_complete += 'N'
                    end_time = datetime.datetime.now()
                    print(str(end_time) + ' ' + i + ' Backup Faild')
                else:
                    if is_complete:
                        is_complete += ',Y'
                    else:
                        is_complete += 'Y'
                    end_time = datetime.datetime.now()
                    logging.info(i + ' End Backup')
                    print(str(end_time) + ' ' + i + ' Backup Complete')
        end_time = datetime.datetime.now()
        elapsed_time = (end_time - start_time).seconds
        #
        full_comm = 'mydumper ' + \
            ' '.join(mydumper_args) + ' database=' + ','.join(bdb_list)
        return start_time, end_time, elapsed_time, is_complete, full_comm, bdb, last_outputdir


def getIP():
    '''获取ip地址'''
    # 过滤内网IP
    cmd = "/sbin/ifconfig  | /bin/grep  'inet addr:' | /bin/grep -v '127.0.0.1' | /bin/grep -v '192\.168' | /bin/grep -v '10\.'|  /bin/cut -d: -f2 | /usr/bin/head -1 |  /bin/awk '{print $1}'"
    child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    child.wait()
    ipaddress = child.communicate()[0].strip()
    return ipaddress


def getMetadata(outputdir):
    '''从metadata中获取 SHOW MASTER STATUS / SHOW SLAVE STATUS 信息'''
    if outputdir[-1] != '/':
        metadata = outputdir + '/metadata'
    else:
        metadata = outputdir + 'metadata'
    with open(metadata, 'r') as file:
        content = file.readlines()

    separate_pos = content.index('\n')

    master_status = content[:separate_pos]
    master_log = [x.split(':')[1].strip() for x in master_status if 'Log' in x]
    master_pos = [x.split(':')[1].strip() for x in master_status if 'Pos' in x]
    master_GTID = [x.split(':')[1].strip()
                   for x in master_status if 'GTID' in x]
    master_info = ','.join(master_log + master_pos + master_GTID)

    slave_status = content[separate_pos + 1:]
    if not 'Finished' in slave_status[0]:
        slave_log = [x.split(':')[1].strip() for x in slave_status if 'Log' in x]
        slave_pos = [x.split(':')[1].strip() for x in slave_status if 'Pos' in x]
        slave_GTID = [x.split(':')[1].strip() for x in slave_status if 'GTID' in x]
        slave_info = ','.join(slave_log + slave_pos + slave_GTID)
        return master_info, slave_info
    else:
        return master_info, 'Not a slave'


def safeCommand(cmd):
    '''移除bk_command中的密码'''
    cmd_list = cmd.split(' ')
    passwd = [x.split('=')[1] for x in cmd_list if 'password' in x][0]
    safe_command = cmd.replace(passwd, 'supersecrect')
    return safe_command


def getVersion(db):
    '''获取mydumper 版本和 mysql版本'''
    child = subprocess.Popen('mydumper --version',
                             shell=True, stdout=subprocess.PIPE)
    child.wait()
    mydumper_version = child.communicate()[0].strip()
    mysql_version = db.version()
    return mydumper_version, mysql_version


def rsync(bk_dir, address):
    '''rsync, bk_dir为备份所在目录,address为使用的网卡'''
    if not address:
        cmd = 'rsync -auv ' + bk_dir + ' --password-file=' + \
            password_file + ' rsync://' + dest
    else:
        cmd = 'rsync -auv ' + bk_dir + ' --address=' + address + \
            ' --password-file=' + password_file + ' rsync://' + dest
    start_time = datetime.datetime.now()
    logging.info('Start rsync')
    print(str(start_time) + ' Start rsync')
    child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    while child.poll() == None:
        stdout_line = child.stdout.readline().strip()
        if stdout_line:
            logging.info(stdout_line)
    logging.info(child.stdout.read().strip())
    state = child.returncode
    if state != 0:
        end_time = datetime.datetime.now()
        logging.critical('Rsync Failed!')
        print(str(end_time) + ' Rsync Failed!')
        is_complete = 'N'
    else:
        end_time = datetime.datetime.now()
        logging.info('Rsync complete')
        print(str(end_time) + ' Rsync complete')
        is_complete = 'Y'
    elapsed_time = (end_time - start_time).seconds
    return start_time, end_time, elapsed_time, is_complete


if __name__ == '__main__':
    '''
    参数解析
    '''
    arguments = docopt(__doc__, version='pybackup 0.3')
    print(arguments)
    if arguments['--no-rsync']:
        rsync = False

    if arguments['--no-history']:
        history = False
    else:
        history = True

    if arguments['--only-backup']:
        history = False
        rsync = False
    else:
        history = True

    if arguments['mydumper'] and ('help' in arguments['ARG_WITH_NO_--'][0]):
        subprocess.call('mydumper --help', shell=True)
    else:
        confLog()
        if arguments['mydumper']:
            mydumper_args = ['--' + x for x in arguments['ARG_WITH_NO_--']]

        bk_dir = [x for x in arguments['ARG_WITH_NO_--'] if 'outputdir' in x][0].split('=')[1]
        targetdb = Fandb(tdb_host, tdb_port, tdb_user, tdb_passwd, tdb_use)
        bk_id = str(uuid.uuid1())
        bk_server = getIP()
        start_time, end_time, elapsed_time, is_complete, bk_command, backuped_db, last_outputdir = runBackup(
            targetdb)
        safe_command = safeCommand(bk_command)
        
        if is_complete:
            bk_size = getBackupSize(bk_dir)
            master_info, slave_info = getMetadata(last_outputdir)
        else:
            bk_size = 'N/A'
            master_info, slave_info = 'N/A', 'N/A'

        if rsync_enable:
            if rsync:
                transfer_start, transfer_end, transfer_elapsed, transfer_complete = rsync(
                    bk_dir, address)
            else:
                transfer_start, transfer_end, transfer_elapsed, transfer_complete = None,None,None,'N/A (local backup)'
                dest = 'N/A (local backup)'

        if history:
            CMDB = Fandb(cm_host, cm_port, cm_user, cm_passwd, cm_use)
            mydumper_version, mysql_version = getVersion(targetdb)
            sql = 'insert into user_backup(bk_id,bk_server,start_time,end_time,elapsed_time,backuped_db,is_complete,bk_size,bk_dir,transfer_start,transfer_end,transfer_elapsed,transfer_complete,remote_dest,master_status,slave_status,tool_version,server_version,bk_command,tag) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            CMDB.insert(sql, (bk_id, bk_server, start_time, end_time, elapsed_time, backuped_db, is_complete, bk_size, bk_dir, transfer_start, transfer_end,
                              transfer_elapsed, transfer_complete, dest, master_info, slave_info, mydumper_version, mysql_version, safe_command, tag))
            CMDB.commit()
            CMDB.close()
