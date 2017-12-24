# pybackup使用文档
pybackup源自于对线上备份脚本的改进和对备份情况的监控需求.

原本生产库的备份是通过shell脚本调用mydumper,之后再将备份通过rsync传输到备份机.

想要获取备份状态,时间,rsync传输时间等信息只能通过解析日志.

pybackup由python编写,调用mydumper和rsync,将备份信息存入数据库中,后期可以通过grafana图形化展示和监控备份

目前不支持2.6,仅在2.7.14做过测试
## 参数说明
帮助信息
```
Usage:
        pybackup.py mydumper ARG_WITH_NO_--... (([--no-rsync] [--no-history]) | [--only-backup])
        pybackup.py only-rsync [--backup-dir=<DIR>] [--bk-id=<id>] [--log-file=<log>]
        pybackup.py mark-del --backup-dir=<DIR>
        pybackup.py validate-backup --log-file=<log>
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
```

### pybackup.py mydumper
```
pybackup.py mydumper ARG_WITH_NO_--... (([--no-rsync] [--no-history]) | [--only-backup])
```
除了最后三个参数,使用的所有参数和mydumper -h中列出的参数相同. 只不过目前只支持长选项,并且不带'--'

例:
```
./pybackup.py mydumper password=fanboshi database=fandb outputdir=/data4/recover/pybackup/2017-11-12 logfile=/data4/recover/pybackup/bak.log verbose=3
```
可以使用`./pybackup.py mydumper help`查看mydumper帮助信息

--no-rsync

不使用rsync传输

--no-history

不记录备份信息到数据库

--only-backup

等价于同时使用--no-rsync和--no-history . 不能与--no-rsync或--no-history同时使用

```
pybackup.py only-rsync [--backup-dir=<DIR>] [--bk-id=<id>] [--log-file=<log>]
```
当备份成功rsync失败时可以使用only-rsync来同步备份成功的文件

--backup-dir

需要使用rsync同步的备份文件路径,如果不指定,则默认为./

--bk-id

user_backup表中记录的备份bk_id,如果指定,则会在rsync同步完成后更新指定bk_id行的,传输起始时间,耗时,是否成功等信息.如果不指定则不更新user_backup表

--log-file

本次rsync指定的日志,如果不指定,则默认为当前目录rsync.log文件

#### 配置文件说明
配置文件为pbackup.conf
```
[root@localhost pybackup]# less pybackup.conf 
[CATALOG]            --存储备份信息的数据库配置
db_host=localhost
db_port=3306
db_user=root
db_passwd=fanboshi
db_use=catalogdb

[TDB]              --需要备份的数据库配置
db_host=localhost
db_port=3306
db_user=root
db_passwd=fanboshi
db_use=information_schema
db_consistency=True  --0.7.0新增option,可以不写,不写则为False,后面会对这个option进行说明
db_list=test,fandb,union_log_ad_% --指定需要备份的数据库,可以使用mysql支持的通配符. 如果想要备份所有数据库则填写%

[rsync]
password_file=/data4/recover/pybackup/rsync.sec    --等同于--password-file
dest=platform@182.92.83.238/db_backup/106.3.130.84 --传输到哪个目录
address=                                           --使用的网卡.可以为空不指定
```
注意
```
[TDB]
db_list=fan,bo,shi           代表备份fan,bo,shi三个数据库
db_list=!fan,!bo,!shi        代表不备份fan,bo,shi三个数据库
db_list=%                    代表备份所有数据库
db_list=!fan,bo              不支持
```
还有一点需要注意,即便在配置文件中定义了db_list参数,也可以在命令行强制指定database=xx / regex / tables-list,例如
```
pybackup.py mydumper password="xx" user=root socket=/data/mysql/mysql.sock outputdir=/data/backup_db/ verbose=3 compress threads=8 triggers events routines use-savepoints logfile=/data/backup_db/pybackup.log database=yourdb
```
此时会只备份`yourdb`而忽略配置文件中的定义

备份信息示例
```
*************************** 4. row ***************************
               id: 4
            bk_id: bcd36dc6-c9e7-11e7-9e30-005056b15d9c
        bk_server: 106.3.130.84
       start_time: 2017-11-15 17:31:20
         end_time: 2017-11-15 17:32:07
     elapsed_time: 47
      backuped_db: fandb,test,union_log_ad_201710_db,union_log_ad_201711_db
      is_complete: Y,Y,Y,Y
          bk_size: 480M
           bk_dir: /data4/recover/pybackup/2017-11-15
   transfer_start: 2017-11-15 17:32:07
     transfer_end: 2017-11-15 17:33:36
 transfer_elapsed: 89
transfer_complete: Y
      remote_dest: platform@182.92.83.238/db_backup/106.3.130.84/
    master_status: mysql-bin.000036,61286,
     slave_status: Not a slave
     tool_version: mydumper 0.9.2, built against MySQL 5.5.53
   server_version: 5.7.18-log
       bk_command: mydumper --password=supersecrect --outputdir=/data4/recover/pybackup/2017-11-15 --verbose=3 --compress --triggers --events --routines --use-savepoints database=fandb,test,union_log_ad_201710_db,union_log_ad_201711_db
```
#### 建库建表语句
```
create database catalogdb;
*************************** 1. row ***************************
       Table: user_backup
Create Table: CREATE TABLE `user_backup` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `bk_id` varchar(36) NOT NULL,
  `bk_server` varchar(15) NOT NULL,
  `start_time` datetime NOT NULL,
  `end_time` datetime NOT NULL,
  `elapsed_time` int(11) NOT NULL,
  `backuped_db` varchar(2048) DEFAULT NULL,
  `is_complete` varchar(200) DEFAULT NULL,
  `bk_size` varchar(10) NOT NULL,
  `bk_dir` varchar(200) NOT NULL,
  `transfer_start` datetime DEFAULT NULL,
  `transfer_end` datetime DEFAULT NULL,
  `transfer_elapsed` int(11) DEFAULT NULL,
  `transfer_complete` varchar(20) NOT NULL,
  `remote_dest` varchar(200) NOT NULL,
  `master_status` varchar(200) NOT NULL,
  `slave_status` varchar(200) NOT NULL,
  `tool_version` varchar(200) NOT NULL,
  `server_version` varchar(200) NOT NULL,
  `pybackup_version` varchar(200) DEFAULT NULL,
  `bk_command` varchar(2048) DEFAULT NULL,
  `tag` varchar(200) NOT NULL DEFAULT 'N/A',
  `is_deleted` char(1) NOT NULL DEFAULT 'N',
  `validate_status` varchar(20) NOT NULL DEFAULT 'N/A',
  PRIMARY KEY (`id`),
  UNIQUE KEY `bk_id` (`bk_id`),
  KEY `idx_start_time` (`start_time`),
  KEY `idx_transfer_start` (`transfer_start`)
) ENGINE=InnoDB AUTO_INCREMENT=178 DEFAULT CHARSET=utf8
```

#### 关于db_consistency
```
./pybackup.py mydumper password=fanboshi database=fandb outputdir=/data4/recover/pybackup/2017-11-12 logfile=/data4/recover/pybackup/bak.log verbose=3
```
以上面命令为例,默认脚本逻辑对于db_list指定的库通过for循环逐一使用mydumper --database=xx 备份
如果知道db_consistency=True则会替换为使用 --regex备份db_list中指定的所有数据库, 保证了数据库之间的一致性

#### 备份脚本示例
```
#!/bin/sh
DSERVENDAY=`date +%Y-%m-%d --date='2 day ago'`
DTODAY=`date +%Y-%m-%d`

cd /data/backup_db/
rm -rf $DTODAY
rm -rf $DSERVENDAY  --不再建议这样删除备份了,建议使用 rm成功后使用 pybackup.py mark-del --bk-id将对应备份信息更新为已删除
mkdir $DTODAY
source ~/.bash_profile
python /data/backup_db/pybackup.py mydumper password="papapa" user=root socket=/data/mysql/mysql.sock outputdir=/data/backup_db/$DTODAY verbose=3 compress threads=8 triggers events routines use-savepoints logfile=/data/backup_db/pybackup.log
```

crontab
```
0 4 * * * /data/backup_db/pybackup.sh>> /data/backup_db/pybackup_sh.log 2>&1 
```

logroatate脚本
```
/data/backup_db/pybackup.log {
      daily
      rotate 7
      missingok
      compress
      delaycompress
      copytruncate
}

/data/backup_db/pybackup_sh.log {
      daily
      rotate 7
      missingok
      compress
      delaycompress
      copytruncate
}
```

### pybackup.py only-rsync
当备份传输失败时,此命令用于手工传输备份,指定bk-id会更新user_backup表

### pybackup.py mark-del
建议使用pybackup备份的备份集先使用此命令更新user_backup.is_deleted列后在物理删除
```
python /data/scripts/bin/pybackup.py mark-del --backup-dir=$obsolete_dir2
find /data2/backup/db_backup/101.37.164.13 -name "2017*"  ! -name  "*-01" -type d  -mtime  +31 -exec rm -r {} \;
```

逻辑是通过找到指定目录下的目录名(目录名就是bk_id),根据此目录名作为bk_id更新user_backup.is_deleted列
```
[root@localhost 2017-12-14]# tree
.
└── 0883fd06-e033-11e7-88ad-00163e0e2396
    └── day_hour
        ├── dbe8je6i4c3gjd50.day-schema.sql.gz
        ├── dbe8je6i4c3gjd50.day.sql.gz
        ├── dbe8je6i4c3gjd50.hour-schema.sql.gz
        ├── dbe8je6i4c3gjd50.hour.sql.gz
        └── metadata
```

### pybackup.py validate-backup
用于测试备份的可恢复性, 通过查询catalogdb库获取未进行恢复测试且未删除的备份进行恢复
需要手工填写user_backup_path表
```
root@localhost 23:12:  [catalogdb]> select * from user_backup_path;
+----+----------------+----------------+-----------------------------------------+--------------------------------+
| id | bk_server      | remote_server  | real_path                               | tag                            |
+----+----------------+----------------+-----------------------------------------+--------------------------------+
|  1 | 120.27.138.23  | 106.3.10.8     | /data1/backup/db_backup/120.27.138.23/  | 国内平台从1                    |
|  2 | 101.37.174.13  | 106.3.10.9     | /data2/backup/db_backup/101.37.174.13/  | 国内平台主2                    |
+----+----------------+----------------+-----------------------------------------+--------------------------------+
```
上例中表示
120.27.138.23(国内平台从1)的备份 存放在 106.3.10.8 的 `/data1/backup/db_backup/120.27.138.23/`中
101.37.174.13(国内平台主2)的备份 存放在 106.3.10.9 的 `/data2/backup/db_backup/101.37.174.13/`中


建议在存放备份的机器安装好MySQL然后通过定时任务不停地查询catalogdb获取需要进行恢复测试的备份集进行恢复,恢复成功后会更新user_backup.validate_status列, 并会在user_recover_info插入记录
```
#备份可恢复性测试
*/15 * * * * /data/scripts/bin/validate_backup.sh >> /data/scripts/log/validate_backup.log 2>&1
[root@localhost 2017-12-14]# less /data/scripts/bin/validate_backup.sh
#!/bin/bash
source ~/.bash_profile
num_validate=`ps -ef | grep pybackup | grep -v grep | grep validate|wc -l`
if [ "$num_validate" == 0 ];then
        python /data/scripts/bin/pybackup.py validate-backup --log-file=/data/scripts/log/validate.log
fi
```

建标语句
```
root@localhost 23:12:  [catalogdb]> show create table user_backup_path\G
*************************** 1. row ***************************
       Table: user_backup_path
Create Table: CREATE TABLE `user_backup_path` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `bk_server` varchar(15) NOT NULL,
  `remote_server` varchar(15) NOT NULL,
  `real_path` varchar(200) NOT NULL,
  `tag` varchar(200) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=13 DEFAULT CHARSET=utf8
1 row in set (0.00 sec)

root@localhost 23:16:  [catalogdb]> show create table user_recover_info\G
*************************** 1. row ***************************
       Table: user_recover_info
Create Table: CREATE TABLE `user_recover_info` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `bk_id` varchar(36) NOT NULL,
  `tag` varchar(200) NOT NULL DEFAULT 'N/A',
  `backup_path` varchar(2000) NOT NULL,
  `db` varchar(200) NOT NULL,
  `start_time` datetime NOT NULL,
  `end_time` datetime NOT NULL,
  `elapsed_time` int(11) NOT NULL,
  `recover_status` varchar(20) DEFAULT NULL,
  `validate_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=27 DEFAULT CHARSET=utf8
1 row in set (0.00 sec)
```

