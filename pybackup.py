#!/usr/bin/python
# -*- coding: utf8 -*-
"""
Usage:
        pybackup.py mydumper ARG_WITH_NO_--... (([--no-rsync] [--no-history]) | [--only-backup])
        pybackup.py only-rsync [--backup-dir=<DIR>] [--bk-id=<id>] [--log-file=<log>]
        pybackup.py mark-del --backup-dir=<DIR>
        pybackup.py validate-backup --log-file=<log> [--bk_id=]
        pybackup.py -h | --help
        pybackup.py --version

Options:
        -h --help                      Show help information.
        --version                      Show version.
        --no-rsync                     Do not use rsync.
        --no-history                   Do not record backup history information.
        --only-backup                  Equal to use both --no-rsync and --no-history.
        --only-rsync                   When you backup complete, but rsync failed, use this option to rsync your backup.
        --backup-dir=<DIR>             The directory where the backuped files are located. [default: ./]
        --bk-id=<id>                   bk-id in table user_backup.
        --log-file=<log>               log file [default: ./pybackup_default.log]

more help information in:
https://github.com/Fanduzi
"""

#示例
"""
python pybackup.py only-rsync --backup-dir=/data/backup_db/2017-11-28 --bk-id=9fc4b0ba-d3e6-11e7-9fd7-00163f001c40 --log-file=rsync.log
--backup-dir 最后日期不带/ 否则将传到rsync://platform@106.3.130.84/db_backup2/120.27.143.36/目录下而不是rsync://platform@106.3.130.84/db_backup2/120.27.143.36/2017-11-28目录下
python /data/backup_db/pybackup.py mydumper password=xx user=root socket=/data/mysql/mysql.sock outputdir=/data/backup_db/2017-11-28 verbose=3 compress threads=8 triggers events routines use-savepoints logfile=/data/backup_db/pybackup.log
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
    if arguments['only-rsync'] or arguments['validate-backup']:
        log = arguments['--log-file']
    else:
        log_file = [x for x in arguments['ARG_WITH_NO_--'] if 'logfile' in x]
        if not log_file:
            print('必须指定--logfile选项')
            sys.exit(1)
        else:
            log = log_file[0].split('=')[1]
            arguments['ARG_WITH_NO_--'].remove(log_file[0])
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        #datefmt='%a, %d %b %Y %H:%M:%S',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename=log,
                        filemode='a')


def getMdumperCmd(*args):
    '''拼接mydumper命令'''
    cmd = 'mydumper '
    for i in range(0, len(args)):
        if i == len(args) - 1:
            cmd += str(args[i])
        else:
            cmd += str(args[i]) + ' '
    return(cmd)


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
        print('getDBS: ' + sql)
        bdb = targetdb.dql(sql)
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
            logging.error('connect error', exc_info=True)

    def dml(self, sql, val=None):
        self.cursor.execute(sql, val)

    def version(self):
        self.cursor.execute('select version()')
        return self.cursor.fetchone()

    def dql(self, sql):
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def commit(self):
        self.conn.commit()

    def close(self):
        self.cursor.close()
        self.diccursor.close()
        self.conn.close()


def runBackup(targetdb):
    '''执行备份'''
    # 是否指定了--database参数
    isDatabase_arg = [ x for x in arguments['ARG_WITH_NO_--'] if 'database' in x ]
    isTables_list = [ x for x in arguments['ARG_WITH_NO_--'] if 'tables-list' in x ]
    isRegex = [ x for x in arguments['ARG_WITH_NO_--'] if 'regex' in x ]
    #备份的数据库 字符串
    start_time = datetime.datetime.now()
    logging.info('Begin Backup')
    print(str(start_time) + ' Begin Backup')
    # 指定了--database参数,则为备份单个数据库,即使配置文件中指定了也忽略

    if isTables_list:
        targetdb.close()
        print(mydumper_args)
        cmd = getMdumperCmd(*mydumper_args)
        cmd_list = cmd.split(' ')
        passwd = [x.split('=')[1] for x in cmd_list if 'password' in x][0]
        cmd = cmd.replace(passwd, '"'+passwd+'"')
        backup_dest = [x.split('=')[1] for x in cmd_list if 'outputdir' in x][0]
        if backup_dest[-1] != '/':
            uuid_dir = backup_dest + '/' + bk_id + '/'
        else:
            uuid_dir = backup_dest + bk_id + '/'

        if not os.path.isdir(uuid_dir):
            os.makedirs(uuid_dir)
        cmd = cmd.replace(backup_dest, uuid_dir)
        child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        while child.poll() == None:
            stdout_line = child.stdout.readline().strip()
            if stdout_line:
                logging.info(stdout_line)
        logging.info(child.stdout.read().strip())
        state = child.returncode
        logging.info('backup state:'+str(state))
        # 检查备份是否成功
        if state != 0:
            logging.critical(' Backup Failed!')
            is_complete = 'N'
            end_time = datetime.datetime.now()
            print(str(end_time) + ' Backup Failed')
        elif state == 0:
            end_time = datetime.datetime.now()
            logging.info('End Backup')
            is_complete = 'Y'
            print(str(end_time) + ' Backup Complete')
        elapsed_time = (end_time - start_time).total_seconds()
        bdb = [ x.split('=')[1] for x in cmd_list if 'tables-list' in x ][0]
        return start_time, end_time, elapsed_time, is_complete, cmd, bdb, uuid_dir, 'tables-list'
    elif isRegex:
        targetdb.close()
        print(mydumper_args)
        cmd = getMdumperCmd(*mydumper_args)
        cmd_list = cmd.split(' ')
        passwd = [x.split('=')[1] for x in cmd_list if 'password' in x][0]
        cmd = cmd.replace(passwd, '"'+passwd+'"')
        regex_expression = [x.split('=')[1] for x in cmd_list if 'regex' in x][0]
        cmd = cmd.replace(regex_expression, "'" + regex_expression + "'")
        backup_dest = [x.split('=')[1] for x in cmd_list if 'outputdir' in x][0]
        if backup_dest[-1] != '/':
            uuid_dir = backup_dest + '/' + bk_id + '/'
        else:
            uuid_dir = backup_dest + bk_id + '/'

        if not os.path.isdir(uuid_dir):
            os.makedirs(uuid_dir)
        cmd = cmd.replace(backup_dest, uuid_dir)
        child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        while child.poll() == None:
            stdout_line = child.stdout.readline().strip()
            if stdout_line:
                logging.info(stdout_line)
        logging.info(child.stdout.read().strip())
        state = child.returncode
        logging.info('backup state:'+str(state))
        # 检查备份是否成功
        if state != 0:
            logging.critical(' Backup Failed!')
            is_complete = 'N'
            end_time = datetime.datetime.now()
            print(str(end_time) + ' Backup Failed')
        elif state == 0:
            end_time = datetime.datetime.now()
            logging.info('End Backup')
            is_complete = 'Y'
            print(str(end_time) + ' Backup Complete')
        elapsed_time = (end_time - start_time).total_seconds()
        bdb = [ x.split('=')[1] for x in cmd_list if 'regex' in x ][0]
        return start_time, end_time, elapsed_time, is_complete, cmd, bdb, uuid_dir, 'regex'
    elif isDatabase_arg:
        targetdb.close()
        print(mydumper_args)
        bdb = isDatabase_arg[0].split('=')[1]
        # 生成备份命令
        database = [ x.split('=')[1] for x in mydumper_args if 'database' in x ][0]
        outputdir_arg = [ x for x in mydumper_args if 'outputdir' in x ]
        temp_mydumper_args = copy.deepcopy(mydumper_args)
        if outputdir_arg[0][-1] != '/':
            temp_mydumper_args.remove(outputdir_arg[0])
            temp_mydumper_args.append(outputdir_arg[0]+'/' + bk_id + '/' + database)
            last_outputdir = (outputdir_arg[0] + '/' + bk_id + '/' + database).split('=')[1]
        else:
            temp_mydumper_args.remove(outputdir_arg[0])
            temp_mydumper_args.append(outputdir_arg[0] + bk_id + '/' + database)
            last_outputdir = (outputdir_arg[0] + bk_id + '/' + database).split('=')[1]
        if not os.path.isdir(last_outputdir):
            os.makedirs(last_outputdir)
        cmd = getMdumperCmd(*temp_mydumper_args)
        #密码中可能有带'#'或括号的,处理一下用引号包起来
        cmd_list = cmd.split(' ')
        passwd = [x.split('=')[1] for x in cmd_list if 'password' in x][0]
        cmd = cmd.replace(passwd, '"'+passwd+'"')
        child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        while child.poll() == None:
            stdout_line = child.stdout.readline().strip()
            if stdout_line:
                logging.info(stdout_line)
        logging.info(child.stdout.read().strip())
        state = child.returncode
        logging.info('backup state:'+str(state))
        # 检查备份是否成功
        if state != 0:
            logging.critical(' Backup Failed!')
            is_complete = 'N'
            end_time = datetime.datetime.now()
            print(str(end_time) + ' Backup Failed')
        elif state == 0:
            end_time = datetime.datetime.now()
            logging.info('End Backup')
            is_complete = 'Y'
            print(str(end_time) + ' Backup Complete')
        elapsed_time = (end_time - start_time).total_seconds()
        return start_time, end_time, elapsed_time, is_complete, cmd, bdb, last_outputdir, 'database'
    # 没有指定--database参数
    elif not isDatabase_arg:
        # 获取需要备份的数据库的列表
        bdb_list = getDBS(targetdb)
        targetdb.close()
        print(bdb_list)
        bdb = ','.join(bdb_list)
        # 如果列表为空,报错
        if not bdb_list:
            logging.critical('必须指定--database或在配置文件中指定需要备份的数据库')
            sys.exit(1)

        if db_consistency.upper() == 'TRUE':
            regex = ' --regex="^(' + '\.|'.join(bdb_list) + '\.' + ')"'
            print(mydumper_args)
            cmd = getMdumperCmd(*mydumper_args)
            cmd_list = cmd.split(' ')
            passwd = [x.split('=')[1] for x in cmd_list if 'password' in x][0]
            cmd = cmd.replace(passwd, '"'+passwd+'"')
            backup_dest = [x.split('=')[1] for x in cmd_list if 'outputdir' in x][0]
            if backup_dest[-1] != '/':
                uuid_dir = backup_dest + '/' + bk_id + '/'
            else:
                uuid_dir = backup_dest + bk_id + '/'
            if not os.path.isdir(uuid_dir):
                os.makedirs(uuid_dir)
            cmd = cmd.replace(backup_dest, uuid_dir)
            cmd = cmd + regex
            child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            while child.poll() == None:
                stdout_line = child.stdout.readline().strip()
                if stdout_line:
                    logging.info(stdout_line)
            logging.info(child.stdout.read().strip())
            state = child.returncode
            logging.info('backup state:'+str(state))
            # 检查备份是否成功
            if state != 0:
                logging.critical(' Backup Failed!')
                is_complete = 'N'
                end_time = datetime.datetime.now()
                print(str(end_time) + ' Backup Failed')
            elif state == 0:
                end_time = datetime.datetime.now()
                logging.info('End Backup')
                is_complete = 'Y'
                print(str(end_time) + ' Backup Complete')
                for db in bdb_list:
                    os.makedirs(uuid_dir + db)
                    os.chdir(uuid_dir)
                    mv_cmd = 'mv `ls ' + uuid_dir + '|grep -v "^' + db + '$"|grep "' + db + '\."` '  + uuid_dir + db + '/'
                    print(mv_cmd)
                    child = subprocess.Popen(mv_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                    while child.poll() == None:
                        stdout_line = child.stdout.readline().strip()
                        if stdout_line:
                            logging.info(stdout_line)
                    logging.info(child.stdout.read().strip())
                    state = child.returncode
                    logging.info('mv state:'+str(state))
                    if state != 0:
                        logging.critical(' mv Failed!')
                        print('mv Failed')
                    elif state == 0:
                        logging.info('mv Complete')
                        print('mv Complete')
                    cp_metadata = 'cp ' + uuid_dir + 'metadata ' + uuid_dir + db + '/'
                    subprocess.call(cp_metadata, shell=True)
            elapsed_time = (end_time - start_time).total_seconds()
            return start_time, end_time, elapsed_time, is_complete, cmd, bdb, uuid_dir, 'db_consistency'
        else:
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
                    temp_mydumper_args.append(outputdir_arg[0] + '/' + bk_id + '/' + i)
                    last_outputdir = (outputdir_arg[0] +'/' + bk_id + '/' + i).split('=')[1]
                else:
                    temp_mydumper_args.remove(outputdir_arg[0])
                    temp_mydumper_args.append(outputdir_arg[0] + bk_id + '/' + i)
                    last_outputdir = (outputdir_arg[0] + bk_id + '/' + i).split('=')[1]
                if not os.path.isdir(last_outputdir):
                    os.makedirs(last_outputdir)
                comm = temp_mydumper_args + ['--database=' + i]
                # 生成备份命令
                cmd = getMdumperCmd(*comm)
                #密码中可能有带'#'或括号的,处理一下用引号包起来
                cmd_list = cmd.split(' ')
                passwd = [x.split('=')[1] for x in cmd_list if 'password' in x][0]
                cmd = cmd.replace(passwd, '"'+passwd+'"')
                child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                while child.poll() == None:
                    stdout_line = child.stdout.readline().strip()
                    if stdout_line:
                        logging.info(stdout_line)
                logging.info(child.stdout.read().strip())
                state = child.returncode
                logging.info('backup state:'+str(state))
                if state != 0:
                    logging.critical(i + ' Backup Failed!')
                    # Y,N,Y,Y
                    if is_complete:
                        is_complete += ',N'
                    else:
                        is_complete += 'N'
                    end_time = datetime.datetime.now()
                    print(str(end_time) + ' ' + i + ' Backup Failed')
                elif state == 0:
                    if is_complete:
                        is_complete += ',Y'
                    else:
                        is_complete += 'Y'
                    end_time = datetime.datetime.now()
                    logging.info(i + ' End Backup')
                    print(str(end_time) + ' ' + i + ' Backup Complete')
        end_time = datetime.datetime.now()
        elapsed_time = (end_time - start_time).total_seconds()
        full_comm = 'mydumper ' + \
            ' '.join(mydumper_args) + ' database=' + ','.join(bdb_list)
        return start_time, end_time, elapsed_time, is_complete, full_comm, bdb, last_outputdir, 'for database'


def getIP():
    '''获取ip地址'''
    # 过滤内网IP
    cmd = "/sbin/ifconfig  | /bin/grep  'inet addr:' | /bin/grep -v '127.0.0.1' | /bin/grep -v '192\.168' | /bin/grep -v '10\.'|  /bin/cut -d: -f2 | /usr/bin/head -1 |  /bin/awk '{print $1}'"
    child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    child.wait()
    ipaddress = child.communicate()[0].strip()
    return ipaddress


def getBackupSize(outputdir):
    '''获取备份集大小'''
    cmd = 'du -sh ' + os.path.abspath(os.path.join(outputdir,'..'))
    child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    child.wait()
    backup_size = child.communicate()[0].strip().split('\t')[0]
    return backup_size


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
    logging.info('rsync state:'+str(state))
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
    elapsed_time = (end_time - start_time).total_seconds()
    return start_time, end_time, elapsed_time, is_complete


def markDel(backup_dir,targetdb):
    backup_list = os.listdir(backup_dir)
    sql = "update user_backup set is_deleted='Y' where bk_id in (" + "'" + "','".join(backup_list) + "')"
    print('markDel:' + sql)
    targetdb.dml(sql)
    targetdb.commit()
    targetdb.close()


def validateBackup(bk_id=None):
    sql = (
    "select a.id, a.bk_id, a.tag, date(start_time), real_path"
    "  from user_backup a,user_backup_path b"
    " where a.tag = b.tag"
    "   and is_complete not like '%N%'"
    "   and is_deleted != 'Y'"
    "   and transfer_complete = 'Y'"
    "   and a.tag = '{}'"
    "   and validate_status != 'passed'"
    "   and start_time >= DATE_SUB(CURDATE(), INTERVAL 1 MONTH)"
    " order by rand() limit 1"
    )

    sql2 = (
    "select a.id, a.bk_id, a.tag, date(start_time), real_path"
    "  from user_backup a,user_backup_path b"
    " where a.tag = b.tag"
    "   and a.bk_id = '{}'"
    )

    start_time, end_time, recover_status, db_list, backup_paths, bk_ids, tags = [], [], [], [], [], [], []
    for tag in bk_list:
        print(datetime.datetime.now())
        print(tag)
        logging.info('-='*20)
        logging.info('开始恢复: ' + tag)
        catalogdb = Fandb(cata_host, cata_port, cata_user, cata_passwd, cata_use)
        if bk_id:
            dql_res = catalogdb.dql(sql2.format(bk_id))
        else:
            dql_res = catalogdb.dql(sql.format(tag))
        result = dql_res[0] if dql_res else None
        if result:
            res_bk_id, res_tag, res_start_time, real_path = result[1], result[2], result[3], result[4]
            catalogdb.close()
            backup_path = real_path + str(res_start_time) + '/' + res_bk_id + '/'
            logging.info('Backup path: '+ backup_path )
            dbs = [ directory for directory in os.listdir(backup_path) if os.path.isdir(backup_path+directory) and directory != 'mysql' ]
            if dbs:
                for db in dbs:
                    '''
                    ([datetime.datetime(2017, 12, 25, 15, 11, 36, 480263), datetime.datetime(2017, 12, 25, 15, 33, 17, 292924), datetime.datetime(2017, 12, 25, 17, 10, 38, 226598), datetime.datetime(2017, 12, 25, 17, 10, 39, 374409)], [datetime.datetime(2017, 12, 25, 15, 33, 17, 292734), datetime.datetime(2017, 12, 25, 17, 10, 38, 226447), datetime.datetime(2017, 12, 25, 17, 10, 38, 855657), datetime.datetime(2017, 12, 25, 17, 10, 39, 776067)], [0, 0, 0, 0], [u'dadian', u'sdkv2', u'dopack', u'catalogdb'], [u'/data2/backup/db_backup/120.55.74.93/2017-12-23/b22694c4-e752-11e7-9370-00163e0007f1/', u'/data2/backup/db_backup/106.3.130.84/2017-12-16/12cb7486-e229-11e7-b172-005056b15d9c/'], [u'b22694c4-e752-11e7-9370-00163e0007f1', u'12cb7486-e229-11e7-b172-005056b15d9c'], ['\xe5\x9b\xbd\xe5\x86\x85sdk\xe4\xbb\x8e1', '\xe6\x96\xb0\xe5\xa4\x87\xe4\xbb\xbd\xe6\x9c\xba'])
                    insert into user_recover_info(tag, bk_id, backup_path, db, start_time, end_time, elapsed_time, recover_status) values (国内sdk从1,b22694c4-e752-11e7-9370-00163e0007f1,/data2/backup/db_backup/120.55.74.93/2017-12-23/b22694c4-e752-11e7-9370-00163e0007f1/,dadian,2017-12-25 15:11:36.480263,2017-12-25 15:33:17.292734,1300.812471,sucess)
                    insert into user_recover_info(tag, bk_id, backup_path, db, start_time, end_time, elapsed_time, recover_status) values (新备份机,12cb7486-e229-11e7-b172-005056b15d9c,/data2/backup/db_backup/106.3.130.84/2017-12-16/12cb7486-e229-11e7-b172-005056b15d9c/,sdkv2,2017-12-25 15:33:17.292924,2017-12-25 17:10:38.226447,5840.933523,sucess)

                    1 个 bk_id 对应3个备份,1 个 bk_id 对应1个备份 ,但是tag只append 了俩, 应该内个库append一次,或者改成字典
                    '''
                    tags.append(tag)
                    backup_paths.append(backup_path)
                    bk_ids.append(res_bk_id)
                    db_list.append(db)
                    full_backup_path = backup_path + db + '/'
                    #print(full_backup_path)
                    load_cmd = 'myloader -d {} --user=root --password=fanboshi --overwrite-tables --verbose=3 --threads=3'.format(full_backup_path)
                    print(load_cmd)
                    start_time.append(datetime.datetime.now())
                    logging.info('Start recover '+ db )
                    child = subprocess.Popen(load_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                    while child.poll() == None:
                        stdout_line = child.stdout.readline().strip()
                        if stdout_line:
                            logging.info(stdout_line)
                    logging.info(child.stdout.read().strip())
                    state = child.returncode
                    recover_status.append(state)
                    logging.info('Recover state:'+str(state))
                    end_time.append(datetime.datetime.now())
                    if state != 0:
                        logging.info('Recover {} Failed'.format(db))
                    elif state == 0:
                        logging.info('Recover {} complete'.format(db))
            else:
                load_cmd = 'myloader -d {} --user=root --password=fanboshi --overwrite-tables --verbose=3 --threads=3'.format(backup_path)
                print(load_cmd)
                tags.append(tag)
                start_time.append(datetime.datetime.now())
                logging.info('Start recover')
                child = subprocess.Popen(load_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                while child.poll() == None:
                    stdout_line = child.stdout.readline().strip()
                    if stdout_line:
                        logging.info(stdout_line)
                logging.info(child.stdout.read().strip())
                state = child.returncode
                recover_status.append(state)
                logging.info('Recover state:'+str(state))
                end_time.append(datetime.datetime.now())
                if state != 0:
                    logging.info('Recover Failed')
                elif state == 0:
                    logging.info('Recover complete')
                db_list.append('N/A')
                backup_paths.append(backup_path)
                bk_ids.append(res_bk_id)
    return start_time, end_time, recover_status, db_list, backup_paths, bk_ids, tags


if __name__ == '__main__':
    '''
    参数解析
    '''
    pybackup_version = 'pybackup 0.10.9.0'
    arguments = docopt(__doc__, version=pybackup_version)
    print(arguments)

    '''
    解析配置文件获取参数
    '''
    cf = ConfigParser.ConfigParser()
    cf.read(os.path.split(os.path.realpath(__file__))[0] + '/pybackup.conf')
#    print(os.getcwd())
#    print(os.path.split(os.path.realpath(__file__))[0])
    section_name = 'CATALOG'
    cata_host = cf.get(section_name, "db_host")
    cata_port = cf.get(section_name, "db_port")
    cata_user = cf.get(section_name, "db_user")
    cata_passwd = cf.get(section_name, "db_passwd")
    cata_use = cf.get(section_name, "db_use")

    if not arguments['validate-backup'] and not arguments['mark-del']:
        section_name = 'TDB'
        tdb_host = cf.get(section_name, "db_host")
        tdb_port = cf.get(section_name, "db_port")
        tdb_user = cf.get(section_name, "db_user")
        tdb_passwd = cf.get(section_name, "db_passwd")
        tdb_use = cf.get(section_name, "db_use")
        tdb_list = cf.get(section_name, "db_list")
        try:
            global db_consistency
            db_consistency = cf.get(section_name, "db_consistency")
        except ConfigParser.NoOptionError,e:
            db_consistency = 'False'
            print('没有指定db_consistency参数,默认采用--database循环备份db_list中指定的数据库,数据库之间不保证一致性')

        if cf.has_section('rsync'):
            section_name = 'rsync'
            password_file = cf.get(section_name, "password_file")
            dest = cf.get(section_name, "dest")
            address = cf.get(section_name, "address")
            if dest[-1] != '/':
                dest += '/'
            rsync_enable = True
        else:
            rsync_enable = False
            print("没有在配置文件中指定rsync区块,备份后不传输")

        section_name = 'pybackup'
        tag = cf.get(section_name, "tag")
    elif arguments['validate-backup']:
        section_name = 'Validate'
        if arguments['--bk_id']:
            bk_list=list(arguments['--bk_id'])
        else:
            bk_list = cf.get(section_name, "bk_list").split(',')

    if arguments['mydumper'] and ('help' in arguments['ARG_WITH_NO_--'][0]):
        subprocess.call('mydumper --help', shell=True)
    elif arguments['only-rsync']:
        confLog()
        backup_dir = arguments['--backup-dir']
        if arguments['--bk-id']:
            transfer_start, transfer_end, transfer_elapsed, transfer_complete = rsync(backup_dir, address)
            catalogdb = Fandb(cata_host, cata_port, cata_user, cata_passwd, cata_use)
            sql = 'update user_backup set transfer_start=%s, transfer_end=%s, transfer_elapsed=%s, transfer_complete=%s where bk_id=%s'
            catalogdb.dml(sql, (transfer_start, transfer_end, transfer_elapsed, transfer_complete, arguments['--bk-id']))
            catalogdb.commit()
            catalogdb.close()
        else:
            rsync(backup_dir,address)
    elif arguments['mark-del']:
        catalogdb = Fandb(cata_host, cata_port, cata_user, cata_passwd, cata_use)
        markDel(arguments['--backup-dir'],catalogdb)
    elif arguments['validate-backup']:
        confLog()
        if arguments['--bk_id']:
            start_time, end_time, recover_status, db_list, backup_paths, bk_ids, tags = validateBackup(arguments['--bk_id'])
        else:
            start_time, end_time, recover_status, db_list, backup_paths, bk_ids, tags = validateBackup()
        print(start_time, end_time, recover_status, db_list, backup_paths, bk_ids, tags)
        if bk_ids:
            catalogdb = Fandb(cata_host, cata_port, cata_user, cata_passwd, cata_use)
            sql1 = "insert into user_recover_info(tag, bk_id, backup_path, db, start_time, end_time, elapsed_time, recover_status) values (%s,%s,%s,%s,%s,%s,%s,%s)"
            sql2 = "update user_backup set validate_status=%s where bk_id=%s"
            logging.info(zip(start_time, end_time, recover_status, db_list))
            for stime, etime, rstatus, db ,backup_path, bk_id, tag in zip(start_time, end_time, recover_status, db_list, backup_paths, bk_ids, tags):
                if rstatus == 0:
                    status = 'sucess'
                    failed_flag = False
                else:
                    status = 'failed'
                    failed_flag = True
#                print(sql1 % (tag.decode('utf-8'), bk_id, backup_path, db, stime, etime, (etime - stime).total_seconds(), status))
                logging.info(sql1 % (tag.decode('utf-8'), bk_id, backup_path, db, stime, etime, (etime - stime).total_seconds(), status))
                catalogdb.dml(sql1,(tag, bk_id, backup_path, db, stime, etime, (etime - stime).total_seconds(), status))
                if not failed_flag:
                    catalogdb.dml(sql2,('passed', bk_id))
                catalogdb.commit()
            catalogdb.close()
            logging.info('恢复完成')
        else:
            logging.info('没有可用备份')
            print('没有可用备份')
    else:
        confLog()
        bk_id = str(uuid.uuid1())
        if arguments['mydumper']:
            mydumper_args = ['--' + x for x in arguments['ARG_WITH_NO_--']]
            is_rsync = True
            is_history = True
            if arguments['--no-rsync']:
                is_rsync = False
            if arguments['--no-history']:
                is_history = False
            if arguments['--only-backup']:
                is_history = False
                is_rsync = False
            print('is_rsync,is_history: ',is_rsync,is_history)
        bk_dir = [x for x in arguments['ARG_WITH_NO_--'] if 'outputdir' in x][0].split('=')[1]
        os.chdir(bk_dir)
        targetdb = Fandb(tdb_host, tdb_port, tdb_user, tdb_passwd, tdb_use)
        mydumper_version, mysql_version = getVersion(targetdb)
        start_time, end_time, elapsed_time, is_complete, bk_command, backuped_db, last_outputdir, backup_type = runBackup(
            targetdb)

        safe_command = safeCommand(bk_command)

        if 'N' not in is_complete:
            bk_size = getBackupSize(last_outputdir)
            master_info, slave_info = getMetadata(last_outputdir)
            if rsync_enable:
                if is_rsync:
                    transfer_start, transfer_end, transfer_elapsed, transfer_complete_temp = rsync(bk_dir, address)
                    transfer_complete = transfer_complete_temp
                    transfer_count = 0
                    while transfer_complete_temp != 'Y' and transfer_count < 3:
                        transfer_start_temp, transfer_end, transfer_elapsed_temp, transfer_complete_temp = rsync(bk_dir, address)
                        transfer_complete = transfer_complete + ',' + transfer_complete_temp
                        transfer_count += 1
                    transfer_elapsed = ( transfer_end - transfer_start ).total_seconds()

                else:
                    transfer_start, transfer_end, transfer_elapsed, transfer_complete = None,None,None,'N/A (local backup)'
                    dest = 'N/A (local backup)'
        else:
            bk_size = 'N/A'
            master_info, slave_info = 'N/A', 'N/A'
            transfer_start, transfer_end, transfer_elapsed, transfer_complete = None,None,None,'Backup failed'
            dest = 'Backup failed'


        if is_history:
            bk_server = getIP()
            catalogdb = Fandb(cata_host, cata_port, cata_user, cata_passwd, cata_use)
            sql = 'insert into user_backup(bk_id,bk_server,start_time,end_time,elapsed_time,backuped_db,is_complete,bk_size,bk_dir,transfer_start,transfer_end,transfer_elapsed,transfer_complete,remote_dest,master_status,slave_status,tool_version,server_version,pybackup_version,bk_command,tag) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            if backup_type == 'for database':
                last_outputdir = os.path.abspath(os.path.join(last_outputdir,'..'))
            print(bk_id, bk_server, start_time, end_time, elapsed_time, backuped_db, is_complete, bk_size, last_outputdir, transfer_start, transfer_end,transfer_elapsed, transfer_complete, dest, master_info, slave_info, mydumper_version, mysql_version, pybackup_version, safe_command)
            catalogdb.dml(sql, (bk_id, bk_server, start_time, end_time, elapsed_time, backuped_db, is_complete, bk_size, last_outputdir, transfer_start, transfer_end,
                              transfer_elapsed, transfer_complete, dest, master_info, slave_info, mydumper_version, mysql_version, pybackup_version, safe_command, tag))
            catalogdb.commit()
            catalogdb.close()

