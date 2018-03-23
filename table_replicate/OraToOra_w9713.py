#!/usr/bin/env python
#coding=utf-8

'''
@2016-10-10
数据表增量复制程序，根据w9713项目的要求定制：

1. 扫描源表数据的条件是不是 send_status=0 ？ 这个状态字段原来我们的定义是char(1) 现在变成了 number(1), 是不是有修改的可能?
        因为原来的同步规则里， 扫描条件send_status='0', 如果同步失败， 还需要设置 send_status='E'
   --  zhaoxg_bj 已经把 send_status 类型可以修改为char(1)
2. 扫描数据是否需要order by?   以哪个字段呢？  因为看到sync_type 里有 C D, 的值， 这个怕有顺序要求， 假设同一id的数据本来的顺序是 C D, 同步顺序错了变成D C, 那后边那条记录就会同步失败。   （assume： C-change D-delete）
  --  order by 字段是   sync_time  TIMESTAMP(6)，可以保证顺序要求

3. sync_type 这个字段的可能值定义， 以及对应的同步处理的说明， 之前liupengc电话里只提到 add update, 这个和样例数据对应不上
  --  sync_type  C-create   D-delete，同步程序无需关注更新操作如何处理，只需做插入和删除操作即可。

4. 源表数据在同步后怎么处理？ 如果要移入历史表， 请提供历史表结构； 如果是修改状态， 请提供修改说明
  --  移入历史表， 历史表结构和对应源表结构字段定义完全一致

样例源表
create table SYNC_BS_SYSUSER_DICT
(
  id               RAW(16),
  sync_time        TIMESTAMP(6),
  send_status      CHAR(1),
  system_user_id   NUMBER(12) not null,
  system_user_code VARCHAR2(250) not null,
  staff_id         NUMBER(12) not null,
  password         VARCHAR2(128),
  status_cd        VARCHAR2(6),
  maintain_flag    VARCHAR2(1),
  relogin_flag     CHAR(1) not null,
  login_status     CHAR(1) not null,
  status_date      DATE,
  create_date      DATE,
  allow_begin      DATE,
  allow_end        DATE,
  pwd_end_date     DATE,
  limit_count      NUMBER(6),
  login_type       VARCHAR2(5),
  login_level_code VARCHAR2(5),
  note             VARCHAR2(100),
  ipbind_flag      CHAR(1) not null,
  ip_address       VARCHAR2(15),
  contract_phone   VARCHAR2(20),
  sendpwd_flag     CHAR(1) not null,
  login_flag       CHAR(1),
  org_code         VARCHAR2(9),
  out_flag         VARCHAR2(20),
  lan_id           NUMBER(12),
  common_region_id NUMBER(12),
  bak1             VARCHAR2(128),
  bak2             VARCHAR2(20),
  bak3             VARCHAR2(20),
  bak4             VARCHAR2(50),
  bak5             VARCHAR2(20),
  out_value        VARCHAR2(100),
  channel_id       NUMBER(12),
  power_right      NUMBER(5),
  tenancy_code     VARCHAR2(4) default ('999'),
  child_flag       VARCHAR2(2),
  creat_optid      VARCHAR2(250),
  sync_type        VARCHAR2(2)
);




目标表  create table BS_SYSUSER_DICT
(
  system_user_id   NUMBER(12) not null,
  system_user_code VARCHAR2(250) not null,
  staff_id         NUMBER(12) not null,
  password         VARCHAR2(128),
  status_cd        VARCHAR2(6),
  maintain_flag    VARCHAR2(1),
  relogin_flag     CHAR(1) not null,
  login_status     CHAR(1) not null,
  status_date      DATE,
  create_date      DATE,
  allow_begin      DATE,
  allow_end        DATE,
  pwd_end_date     DATE,
  limit_count      NUMBER(6),
  login_type       VARCHAR2(5),
  login_level_code VARCHAR2(5),
  note             VARCHAR2(100),
  ipbind_flag      CHAR(1) not null,
  ip_address       VARCHAR2(15),
  contract_phone   VARCHAR2(20),
  sendpwd_flag     CHAR(1) not null,
  login_flag       CHAR(1),
  org_code         VARCHAR2(9),
  out_flag         VARCHAR2(20),
  lan_id           NUMBER(12),
  common_region_id NUMBER(12),
  bak1             VARCHAR2(128),
  bak2             VARCHAR2(20),
  bak3             VARCHAR2(20),
  bak4             VARCHAR2(50),
  bak5             VARCHAR2(20),
  out_value        VARCHAR2(100),
  channel_id       NUMBER(12),
  power_right      NUMBER(5),
  tenancy_code     VARCHAR2(4) default ('999'),
  child_flag       VARCHAR2(2),
  creat_optid      VARCHAR2(250)
);

import cx_Oracle
lines = file('d:/temp/SYNC_BS_SYSUSER_DICT.txt').read().split('\n')
sql = """insert into SYNC_BS_SYSUSER_DICT values(
:v0, to_timestamp(:v1, 'yyyy-mm-dd hh24:mi:ss:ff'),
:v2,:v3,:v4,:v5,:v6,:v7,:v8,:v9,:v10,to_date(:v11,'yyyy-mm-dd hh24:mi:ss'),to_date(:v12,'yyyy-mm-dd hh24:mi:ss'),:v13,:v14,to_date(:v15,'yyyy-mm-dd'),:v16,:v17,:v18,:v19,:v20,:v21,:v22,:v23,:v24,:v25,:v26,:v27,:v28,:v29,:v30,:v31,:v32,:v33,:v34,:v35,:v36,:v37,:v38,:v39,:v40)"""
co = cx_Oracle.connect('DBOFFONADM/dbaccopr200606@centdb')
cur = co.cursor()
for line in lines: cur.execute(sql, line.split('\t') )
co.commit()

select id,sync_time,system_user_id, system_user_code,sync_type from SYNC_BS_SYSUSER_DICT order by sync_time,id
select * from SYNC_BS_SYSUSER_DICT_HIS for update
select * from BS_SYSUSER_DICT

insert into SYNC_BS_SYSUSER_DICT select * from SYNC_BS_SYSUSER_DICT_HIS;
truncate table SYNC_BS_SYSUSER_DICT_HIS;
truncate table BS_SYSUSER_DICT;

公共配置文件(包括日志级别 和 数据库连接参数)存放位置: etc/datacopy.conf
表复制的配置文件保存在目录 etc_incr/ 中, 一个进程一个配置文件, 文件名后缀: .conf
日志文件记录在 log/ 下, 以对应的复制配置文件名为前缀 


下面是两个示例配置文件 内容:
=================etc_incr/cp1.conf BEGIN
[source]
database: dbcrma
tablename: SYNC_BS_SYSUSER_DICT
his_table: SYNC_BS_SYSUSER_DICT_HIS
columns: system_user_id, system_user_code, staff_id, password, status_cd, maintain_flag, relogin_flag, login_status, status_date, create_date, allow_begin, allow_end, pwd_end_date, limit_count, login_type, login_level_code, note, ipbind_flag, ip_address, contract_phone, sendpwd_flag, login_flag, org_code, out_flag, lan_id, common_region_id, bak1, bak2, bak3, bak4, bak5, out_value, channel_id, power_right, tenancy_code, child_flag, creat_optid
order_by: sync_time
status_column: send_status
id_column: id
oper_column: sync_type

[target]
database: dbcrma[,dbcrmc...]
tablename: BS_SYSUSER_DICT
id_column: system_user_id

; 说明：
; 1. his_table 的字段与源表相同， 直接插入即可
; 2. oper_column 字段的取值： 'C'-create 'D'-delete
; 3. status_column 字段的取值： '0'-ready 'E'-error
; 4. his_table 名称中如果有 ${yyyymm} 或者 ${yyyymmdd} , 会被以年月[日]的具体数字替换掉
=====================etc_incr/cp1.conf END

=====================etc/datacopy.conf BEGIN
[global]
processtimeout: 60

[logging]
format: %(asctime)s - %(name)s - %(levelname)s - %(message)s

; CRITICAL    50, ERROR    40,  WARNING    30,  INFO    20, DEBUG    10, NOTSET    0
loglevel: 10
filerotate: D

[db1]
type: oracle
conn: {b64}Y3JtLGNybSwxMjcuMC4wLjEsMzMwNix0ZXN0

[dbcrma]
type: oracle
conn: dbcustopr/dbcustopr@crma
===================etc/datacopy.conf END

'''

import os
import sys
import ConfigParser
import base64
import time
import logging
import logging.config
import logging.handlers

__test_flag__ = True

logger = None
def initlogger(lname, dccfg):
    global logger
    loglevel = dccfg.getint("logging", "loglevel")
    logger = logging.getLogger(lname)
    logger.setLevel(loglevel)

    fh = logging.handlers.TimedRotatingFileHandler("log/tc_%s.log"%lname, when=dccfg.get("logging", "filerotate"))
    #ch.setLevel(loglevel)
    fmtstr = dccfg.get("logging", "format")
    #print "===", fmtstr
    formatter = logging.Formatter(fmtstr)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

BLOB_TYPE = None
CLOB_TYPE = None
class TableCopy:
    def __init__(self, copycfgname):
        self.dbsrc = None
        self.dbdst = None
        self.copyname = copycfgname
        self.loadcfg()
        self.pid = os.getpid()
        #self.has_blob = False  # if has_lob, the fetchmany must 1 record per time
        
        initlogger(self.copyname, self.cfg)
        self.ins_mustlist = False  # though, insert many into oracle, must be a list, but the mysql's fetchmany result is tuple

    def loadcfg(self):
        self.cfg = ConfigParser.RawConfigParser()
        conf_file = "etc_incr/"+self.copyname+".conf"
        if not os.path.exists(conf_file):
            raise Exception("file: [%s] not exists"%conf_file)
        self.cfg.read(conf_file)
        
        self.dbcfg = ConfigParser.RawConfigParser()
        #cfgfile = "/crmpdpp/hcai/Eai/plugin/m/ah/appintegration/dbsyn/tblcp/etc/datacopy.conf"
        #cfgfile = "/crmpdpp/hcai/.bossconfig"
        if not __test_flag__:
            cfgfile="/etc/.ngpasswordconfig"
            #cfgfile="/bosspic/sxeai/xroad/python_add/mysql/cfg/.bossconfig"
            print(cfgfile)
            self.dbcfg.read(cfgfile)

    def conndb(self, dbname):
        #tp = self.dbcfg.get(dbname, "type")
        tp = 'oracle'
        global BLOB_TYPE
        global CLOB_TYPE

        if __test_flag__:
            connstr = "DBOFFONADM/dbaccopr200606@centdb"
        else:
            decrypt_pro = "/bosspic/sxeai/xroad/python_add/mysql/cfg/decrypt_pro"
            print(decrypt_pro)
            get_dbinfo_srv = '%s %s %s' % (decrypt_pro, dbname, 0)
            get_dbinfo_user = '%s %s %s' % (decrypt_pro,dbname, 1)
            get_dbinfo_pwd = '%s %s %s' % (decrypt_pro, dbname, 2)
            p_pipe = os.popen(get_dbinfo_srv)
            dbsrv = p_pipe.read()
            p_pipe.close()
            p_pipe = os.popen(get_dbinfo_user)
            dbuser = p_pipe.read()
            p_pipe.close()
            p_pipe = os.popen(get_dbinfo_pwd)
            dbpwd = p_pipe.read()
            p_pipe.close()
            connstr = '%s/%s@%s' % (dbuser, dbpwd, dbsrv)

        if connstr.startswith('{b64}'):
            connstr = base64.decodestring(connstr[5:])
            print(connstr);
        import cx_Oracle
        BLOB_TYPE = cx_Oracle.BLOB
        CLOB_TYPE = cx_Oracle.CLOB
        conn = cx_Oracle.connect(connstr)
        def OutputTypeHandler(cursor, name, defaultType, size, precision, scale):
            if defaultType == cx_Oracle.CLOB:
                return cursor.var(cx_Oracle.LONG_STRING, 80000, cursor.arraysize)
            if defaultType == cx_Oracle.BLOB:
                return cursor.var(cx_Oracle.LONG_BINARY, 100004, cursor.arraysize)
            if defaultType == cx_Oracle.BINARY:
                return cursor.var(cx_Oracle.STRING, 64, cursor.arraysize)
        conn.outputtypehandler = OutputTypeHandler
        logger.info("connected to database, name: %s  type: %s", dbname, tp)
        return conn, tp
    
    def closeall(self):
        try:
            self.dbsrc.rollback()
        except: pass
        try:
            self.dbsrc.close()
        except: pass
        try:
            for db in self.dbdst: db.rollback()
        except: pass
        try:
            for db in self.dbdst: db.close()
        except: pass

    def copyinit(self):
        logger.info("trying connect to db")
        cfg = self.cfg
      
        src_tbl = cfg.get("source", "tablename")
        
        #构造源数据库的操作sql语句
        src_clmns = ",".join( [
            cfg.get("source", "id_column"),
            cfg.get("source", "oper_column"),
            cfg.get("source", "order_by"),
            cfg.get("source", "status_column"),
            cfg.get("source", "columns") ] )
        self.sql_select = "select %s from %s where %s='0' order by %s" %(
            src_clmns, src_tbl,
            cfg.get("source", "status_column"), cfg.get("source", "order_by")
        )
        #print self.sql_select

        self.sql_ins_his = "insert into %s(%s) values(%s)"%(
            cfg.get("source", "his_table"), src_clmns,
            ",".join( [":v%d"%i for i in range(len(src_clmns.split(",")))] )
        )
        self.sql_del = "delete from %s where %s=:v0"%(src_tbl, cfg.get("source", "id_column") )
        self.sql_update_error = "update %s set %s='E' where %s=:v0"%(
            src_tbl, cfg.get("source", "status_column"), cfg.get("source", "id_column")
        )

        #构造目标数据库的操作语句
        dst_clnms = cfg.get("source", "columns")
        self.sql_ins_dst = "insert into %s(%s) values(%s)"%(
            cfg.get("target", "tablename"),  dst_clnms,
            ",".join([":v%d" % i for i in range(len(dst_clnms.split(",")))])
        )
        id_clmns =  cfg.get("target", "id_column").split(",") # 支持删除条件包括多个字段
        self.sql_del_dst = "delete from %s where %s"%(
            cfg.get("target", "tablename"), " and ".join(("%s=:v%d"%(id_clmns[i], i) for i in xrange(len(id_clmns))))
        )

        k_pos = []
        for k in id_clmns:
            k_pos.append(len(dst_clnms[0:dst_clnms.upper().find(k.strip().upper())].split(',')) - 1)
        self.dst_key_pos = k_pos

        logger.info("Initial the parameters finished.")

        self.dbsrc, _ = self.conndb(cfg.get("source", "database"))
        dbdst = []
        for label in cfg.get("target", "database").strip().split(","):
            db1, _ = self.conndb(label)
            dbdst.append(db1)
        self.dbdst = dbdst

        return True

    def copyonce_st(self):
        cur1 = self.dbsrc.cursor()
        cur1.execute(self.sql_select)

        cur2_s = [db.cursor() for db in self.dbdst ]
        cur3 = self.dbsrc.cursor()

        sql_ins_his = self.sql_ins_his

        sql_del = self.sql_del
        sql_update_error = self.sql_update_error

        sql_ins_dst = self.sql_ins_dst
        sql_del_dst = self.sql_del_dst
        dst_key_pos = [i+4 for i in self.dst_key_pos ]

        copycount = 0

        db_types = [d[1] for d in cur1.description]
        try:
            db_types.index(BLOB_TYPE)
            has_blob = True
        except:
            has_blob = False
        #print '---has_blob', has_blob
        while 1:
            row = cur1.fetchone()
            if row is None:
                break
            copycount += 1
            try:
                # then insert the destination table
                if has_blob: cur3.setinputsizes(*db_types)

                cur3.execute(sql_ins_his, row)
                cur3.execute(sql_del, (row[0], ))

                oper = row[1].strip()
                #print '---', oper, [row[i] for i in dst_key_pos ]
                if oper == 'C':
                    if has_blob:
                        for cur2 in cur2_s: cur2.setinputsizes(*db_types[4:])
                    for cur2 in cur2_s:
                        cur2.execute(sql_ins_dst, row[4:])
                        #print '==', cur2.rowcount
                elif oper == 'D':
                    for cur2 in cur2_s:
                        cur2.execute(sql_del_dst, [row[i] for i in dst_key_pos ] )
                        #print '==', cur2.rowcount
                else:
                    print "!! nothing"
            except Exception,ex:
                #失败后，更新源记录状态为 send_status='E'
                print("#### ex",ex)
                for db in self.dbdst: db.rollback()
                self.dbsrc.rollback()
                print sql_ins_his
                print repr(row)
                cur3.execute(sql_update_error, (row[0], ) )
                logger.error("onecopy faild: %d %s %s", copycount, row[0],ex)
            for db in self.dbdst: db.commit()
            self.dbsrc.commit()

        logger.info( "success finished onecopy, %d", copycount)

        cur1.close()
        for cur2 in cur2_s: cur2.close()
        cur3.close()
        return copycount
     
    def run(self):
        while not self.copyinit(): time.sleep(2.0)
        while True:
            ''''t = raw_input('wait(q to exit):')
            if t.strip() == 'q':
                break'''
            count = 0

            count = self.copyonce_st()
            print 'copy once', count

            if count == 0:
                time.sleep(5.0)
            else:
                time.sleep(1.0)

        self.closeall()
            
    
if __name__ == '__main__':
    cn = "cp1"
    if len(sys.argv) > 1:
        cn = sys.argv[1]
    TableCopy(cn).run()
