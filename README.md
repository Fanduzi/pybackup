# pybackup使用文档
pybackup源自于对线上备份脚本的改进和对备份情况的监控需求.

原本生产库的备份是通过shell脚本调用mydumper,之后再将备份通过rsync传输到备份机.

想要获取备份状态,时间,rsync传输时间等信息只能通过解析日志.

pybackup由python编写,调用mydumper和rsync,将备份信息存入数据库中,后期可以通过grafana图形化展示和监控备份

## 参数说明
```
pybackup.py mydumper ARG_WITH_NO_--... (([--no-rsync] [--no-history]) | [--only-backup])
```
除了最后三个参数,使用的所有参数和mydumper -h中列出的参数相同. 只不过目前只支持长选项,并且不带'--'
列:
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

## 配置文件说明
配置文件为pbackup.conf
```
[root@localhost pybackup]# less pybackup.conf 
[CMDB]            --存储备份信息的数据库配置
db_host=localhost
db_port=3306
db_user=root
db_passwd=fanboshi
db_use=cmdb

[TDB]              --需要备份的数据库配置
db_host=localhost
db_port=3306
db_user=root
db_passwd=fanboshi
db_use=information_schema
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
还有一点需要注意,即便在配置文件中定义了db_list参数,也可以在命令行强制指定database=xx选项,例如
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
建库建表语句
```
create database cmdb;
CREATE TABLE user_backup (
    id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
    bk_id CHAR(36) NOT NULL UNIQUE KEY,
    bk_server VARCHAR(15) NOT NULL,
    start_time DATETIME NOT NULL,
    end_time DATETIME NOT NULL,
    elapsed_time INT NOT NULL,
    backuped_db VARCHAR(200) NOT NULL,
    is_complete VARCHAR(30) NOT NULL,
    bk_size VARCHAR(10) NOT NULL,
    bk_dir VARCHAR(200) NOT NULL,
    transfer_start DATETIME,
    transfer_end DATETIME,
    transfer_elapsed INT,
    transfer_complete VARCHAR(20) NOT NULL,
    remote_dest VARCHAR(200) NOT NULL,
    master_status VARCHAR(200) NOT NULL,
    slave_status VARCHAR(200) NOT NULL,
    tool_version VARCHAR(200) NOT NULL,
    server_version VARCHAR(200) NOT NULL,
    bk_command VARCHAR(400) NOT NULL,
    tag varchar(200) NOT NULL DEFAULT 'N/A' 
)  ENGINE=INNODB CHARACTER SET UTF8 COLLATE UTF8_GENERAL_CI;
```
