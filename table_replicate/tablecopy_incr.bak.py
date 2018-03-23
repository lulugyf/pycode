#!/usr/bin/env python
#coding=utf-8

'''
@2012-10
数据表增量复制程序，通过额外的表记录复制断点， 有以下特性：
1. 表名以及字段名通过配置提供，连接可配置, 一个进程处理一对表复制
2. 源表数据处理提供两种方式: 修改状态字段 或 插入历史表后删除
3. 需提供id字段 和 状态字段, 只复制状态为 '0' 的记录
4. 使用批量操作方式
5. 如果使用插入历史表方式处理, 则历史表字段使用源表相同的字段
6. 支持lob字段


公共配置文件(包括日志级别 和 数据库连接参数)存放位置: etc/datacopy.conf
表复制的配置文件保存在目录 etc_incr/ 中, 一个进程一个配置文件, 文件名后缀: .conf
日志文件记录在 log/ 下, 以对应的复制配置文件名为前缀 


下面是两个示例配置文件 内容:
=================etc_incr/cp1.conf BEGIN
[source]
database: dbcrma
tablename: src_tbl
columns: col1, col2, st, col3

[target]
database: dbcrma
tablename: dst_tbl
columns: col1, col2, st, col3

[property]
id_column: col1[,col2[,col3]]
status_column: st
his_table: src_tbl_his[${YYYYMM}]
his_columns: col1, st, col2   ;可选， 字段名须与源表相同
fetchmany: 100

; create table src_tbl(col1 number(12) primary key, col2 char(30), st char(1), col3 char(40));
; create table dst_tbl(col1 number(12) primary key, col2 char(30), st char(1), col3 char(40));
; create sequence seq_test1;
; create index idx1_srctbl on src_tbl(st);
; create table src_tbl_his(col1 number(12) primary key, col2 char(30), st char(1), col3 char(40));
; insert into src_tbl select seq_test1.nextval, '111111111111', '0', '9999999' from dual;
; insert into src_tbl select seq_test1.nextval, '111111111111', '0', '9999999' from src_tbl where rownum<=2;

; 说明： 
; 1. 如果配置了  his_table， 则是将源表数据删除并插入历史表, 
            否则只修改状态字段(status_column)的值, 默认情况下, 
            历史表的表结构要求与目标表相同, 插入的字段使用源表的 columns 配置
         (2013-01-07)历史表表名中如果包含 ${YYYYMM}, 则会使用 当前时间的年月替换
         (2013-01-29)如果历史表字段与目标表不同（配置中的），需要单独配置 [properties]his_columns
                                                         其中的字段需要全部包含在源表取出字段中
; 2. source 和  target 下的column配置项， 按顺序对应
; 3. database配置指向 etc/datacopy.conf 中的配置
; 4. (2013-01-07)id_column 允许多个字段做联合索引， 字段名间以，分隔， 字段间的条件关系为 and
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
import datetime
import logging
import logging.config
import logging.handlers
import traceback


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
LOB_TYPE = None
class TableCopy:
    def __init__(self, copycfgname):
        self.dbsrc = None
        self.dbdst = None
        self.copyname = copycfgname
        self.loadcfg()
        self.pid = os.getpid()
        #self.has_blob = False  # if has_lob, the fetchmany must 1 record per time
        
        initlogger(self.copyname, self.dbcfg)
        self.ins_mustlist = False  # though, insert many into oracle, must be a list, but the mysql's fetchmany result is tuple

    def loadcfg(self):
        self.cfg = ConfigParser.RawConfigParser()
        self.cfg.read("etc_incr/"+self.copyname+".conf")
        
        self.dbcfg = ConfigParser.RawConfigParser()
        cfgfile = "etc/datacopy.conf"
        self.dbcfg.read(cfgfile)

    def conndb(self, dbname):
        #tp = self.dbcfg.get(dbname, "type")
        global BLOB_TYPE
        global CLOB_TYPE
        global LOB_TYPE
        tp = "oracle"
        #modify by kuangwf 2013-3-21 11:18:33  begin
        #need modify
        decrypt_pro = "/bfua/basc/increment/tblcp/all/pass/dbdecrypt "
        #decrypt_pro = "/home/glba/dev/tblcp/pass/dd"
        get_dbinfo_srv = '%s %s %s' % (decrypt_pro, dbname, '0')
        get_dbinfo_user = '%s %s %s' % (decrypt_pro,dbname, '1')
        get_dbinfo_pwd = '%s %s %s' % (decrypt_pro, dbname, '2')                    
        p_pipe = os.popen(get_dbinfo_srv)
        dbsrv = p_pipe.read()
        p_pipe.close()
        
                        
        p_pipe = os.popen(get_dbinfo_user)
        dbuser = p_pipe.read()
        p_pipe.close()    
        
        p_pipe = os.popen(get_dbinfo_pwd)
        dbpwd = p_pipe.read()
        p_pipe.close()
        logger.debug("connstr: %s", dbpwd)
        connstr = '%s/%s@%s' % (dbuser, dbpwd, dbsrv) 
        logger.debug("connstr: %s", connstr)     
        #connstr = self.dbcfg.get(dbname, "conn")
                    #modify by kuangwf 2013-3-21 11:18:57  end        
        if connstr.startswith('{b64}'):
            connstr = base64.decodestring(connstr[5:])
        if tp == "oracle":
            import cx_Oracle
            BLOB_TYPE = cx_Oracle.BLOB
            CLOB_TYPE = cx_Oracle.CLOB
            LOB_TYPE = cx_Oracle.LOB
            conn = cx_Oracle.connect(connstr)
            def OutputTypeHandler(cursor, name, defaultType, size, precision, scale):
                if defaultType == cx_Oracle.CLOB:
                    return cursor.var(cx_Oracle.LONG_STRING, 128000, cursor.arraysize)
                if defaultType == cx_Oracle.BLOB:
                    return cursor.var(cx_Oracle.LONG_BINARY, 128000, cursor.arraysize)
            #conn.outputtypehandler = OutputTypeHandler
            self.outhandler = OutputTypeHandler
        else:
            logger.error("not support database type %s", tp)
            return None, tp
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
            self.dbdst.rollback()
        except: pass
        try:
            self.dbdst.close()
        except: pass


    def copyinit(self):
        logger.info("trying connect to db")
        cfg = self.cfg
      
        src_tbl = cfg.get("source", "tablename")
        src_clmns = cfg.get("source", "columns")
        dst_tbl = cfg.get("target", "tablename")
        dst_clmns = cfg.get("target", "columns")
        id_clmn = cfg.get("property", "id_column")
        time_clmn=cfg.get("property", "time_column")
        order_clmn=cfg.get("property", "order_column")
        logger.debug("time_clmn: %s", time_clmn)
        
        #寻找 id字段在列表中的位置
        clmns = [ clmn.strip().upper() for clmn in src_clmns.split(',') ]
        self.key_pos = [clmns.index(idc.strip().upper()) for idc in id_clmn.split(',') ]
        self.fetchmany = cfg.get("property", "fetchmany")
        self.time_pos=clmns.index(time_clmn.strip().upper())
        logger.debug("time_clmn: %d", self.time_pos) 
        status_clmn = cfg.get("property", "status_column")
        his_tbl = cfg.get("property", "his_table")
        if his_tbl == '':
            mode_status = True  #without history table
        else:
            mode_status = False

        if mode_status:
            #修改为按配置字段排序
            #self.sql_select = "select %s from %s where %s='0' order by %s"%(
            #            src_clmns, src_tbl, status_clmn, id_clmn)
            self.sql_select = "select %s from %s where %s='0' order by %s"%(
                        src_clmns, src_tbl, status_clmn, order_clmn)
            logger.debug("select_sql: %s", self.sql_select)
            self.sql_update = "update %s set %s=:v00 where %s"%(
                    src_tbl, status_clmn,
                     " and ".join([ "%s=:%s"%(cl,cl) for cl in id_clmn.split(',') ])
                     )
            logger.debug("sql_update: %s", self.sql_update)

            vs = []
            for i in range(len(dst_clmns.split(','))):
                vs.append(':v%d'%(i+1))
            self.sql_insert = "insert into %s(%s) values(%s)"%(dst_tbl, dst_clmns, ",".join(vs))
            logger.debug("sql_insert: %s", self.sql_insert)
            
        else:
            #修改为按配置字段排序
            #self.sql_select = "select %s from %s where %s='0' order by %s"%(
            #            src_clmns, src_tbl, status_clmn, id_clmn)
            self.sql_select = "select %s from %s where %s='0' order by %s"%(
                        src_clmns, src_tbl, status_clmn, order_clmn)
            logger.debug("sql_select: %s", self.sql_select)
            
            his_columns = cfg.get("property", "his_columns")
            if his_columns == '':
                vs = []
                for i in range(len(dst_clmns.split(','))):
                    vs.append(':v%d'%(i+1))
                self.sql_inshis = "insert into %s(%s) values(%s)"%(his_tbl, dst_clmns, ",".join(vs))
                self.his_clmns = None
            else:
                # 历史表的结构与目标表不同， 需要根据历史表的字段重新整理插入数据， 这个要求历史表的字段全包含于源表取出的字段中
                vs = []
                for i in range(len(his_columns.split(','))):
                    vs.append(':v%d'%(i+1))
                self.sql_inshis = "insert into %s(%s) values(%s)"%(his_tbl, his_columns, ",".join(vs))
                #然后检查插入字段在源表字段中的位置， 需要在运行期间重新组织结果集
                vn = [c.strip() for c in src_clmns.split(',')]
                self.his_clmns = [vn.index(c.strip()) for c in his_columns.split(',')]
            logger.debug("sql_inshis: %s", self.sql_inshis)
            self.sql_delete = "delete from %s where %s"%(src_tbl,
                    " and ".join([ "%s=:%s"%(cl,cl) for cl in id_clmn.split(',') ])
                            )
            logger.debug("sql_delete: %s", self.sql_delete)

            self.sql_update_error = "update %s set %s='E' where %s"%(
                    src_tbl, status_clmn,
                    " and ".join([ "%s=:%s"%(cl,cl) for cl in id_clmn.split(',') ])
                            )  #出错后更新状态为 E
        
            vs = []
            for i in range(len(dst_clmns.split(','))):
                vs.append(':v%d'%(i+1))
            self.sql_insert = "insert into %s(%s) values(%s)"%(dst_tbl, dst_clmns, ",".join(vs))
            logger.debug("sql_insert: %s", self.sql_insert)
            
        logger.info("Initial the parameters finished.")
        
        self.dbsrc, _ = self.conndb(cfg.get("source", "database"))
        self.dbdst, _ = self.conndb(cfg.get("target", "database"))
        
        self.cp_mode = mode_status
        return True

    def finddulkey(self, ids, cur2):
        cur2.execute(self.sql_maxid)
        maxid = cur2.fetchone()[0]
        i = 0
        for id in ids:
            i += 1
            if maxid < id[0]: break
        return i-1

    def fetch(self, cur1, many, lob_index):
        if hasattr(self, 'outhandler'):
            self.dbsrc.outputtypehandler = self.outhandler
            #logger.debug("=========set outhandler")
        try:
            rows = cur1.fetchmany(many)
        except Exception,ex:
            exstr = str(ex)
            if exstr.find('fetched with error: 1406') >= 0:
                # 只有在结果集被trunk的时候，需要把 outputtypehandler 去掉， 等待重新连接
                # 这里有个没搞明白的问题： 为什么这样做之后重连就可以？ 似乎重连后outhandler 也还是设置上的
                self.dbsrc.outputtypehandler = None
            raise(ex)
        return rows

    def copyonce_st(self):
        cur1 = self.dbsrc.cursor()
        cur1.execute(self.sql_select)
        cur2 = self.dbdst.cursor()
        cur3 = self.dbsrc.cursor()
        
        sql_update = self.sql_update
        sql_insert = self.sql_insert
        
        key_pos = self.key_pos
        time_pos = self.time_pos
        copycount = 0
        many = int(self.fetchmany)
        
        db_types, lob_index = self.checkcoltypes(cur1)
        #print '===', lob_index, repr(db_types)

        while 1:
            rows = self.fetch(cur1, many, lob_index)
            #else:
            #    rows = cur1.fetchmany(many)
            onecount = len(rows)
            if onecount == 0:
                break
            copycount += onecount
            
            try:
                # then insert the destination table
                if lob_index > -1: cur2.setinputsizes(*db_types)
                rows1 = []
                send_time=datetime.datetime.now()
                for r in rows:
                    r1 = [i for i in r]
                    r1[time_pos] = send_time
                    rows1.append(r1)
                    read_flag = False
                    if lob_index > -1 and type(r1[lob_index]) == LOB_TYPE:
                        read_flag = True
                        r1[lob_index] = r1[lob_index].read()
                    logger.debug("rowid: %s read:%s", r1[key_pos[0]], read_flag)
                cur2.executemany(sql_insert, rows1)
                logger.debug("insert rows: %d", cur2.rowcount)
                
                # update the status
                ids = [ [ r[posa] for posa in key_pos ] for r in rows]
                for r in ids: r.insert(0, '1')
                cur3.executemany(sql_update, ids)
            except:
                logger.error("5failed %s", ''.join(traceback.format_exception(*sys.exc_info())))
                #如果批量插入失败， 则一条条插入，对于失败的，把status标记为 E
                self.dbdst.rollback()
                self.dbsrc.rollback()
                for r in rows:
                    if lob_index > -1: cur2.setinputsizes(*db_types)
                    try:
                        cur2.execute(sql_insert, r)
                        flag = '1'
                    except:
                        flag = 'E' #failed flag
                    ids = [r[posa] for posa in key_pos]
                    ids.insert(0, flag)
                    cur3.execute(sql_update, ids)

            self.dbdst.commit()
            self.dbsrc.commit()

        logger.info( "success finished onecopy, %d", copycount)

        cur1.close()
        cur2.close()
        cur3.close()
        return copycount

    def checkcoltypes(self, cur1):
        db_types = [d[1] for d in cur1.description]
        lob_index = -1
        try:
            lob_index = db_types.index(BLOB_TYPE)
        except: pass
        try:
            lob_index = db_types.index(CLOB_TYPE)
        except: pass
        return db_types, lob_index

    def copyonce_his(self):
        cur1 = self.dbsrc.cursor()
        cur1.execute(self.sql_select)
        cur2 = self.dbdst.cursor()
        cur3 = self.dbsrc.cursor()
        sql_insert = self.sql_insert
        sql_inshis = self.sql_inshis
        sql_delete = self.sql_delete
        sql_update_error = self.sql_update_error
        
        key_pos = self.key_pos
        time_pos = self.time_pos
        
        copycount = 0
        many = int(self.fetchmany)
        his_clmns = self.his_clmns
        
        db_types, lob_index = self.checkcoltypes(cur1)

        if lob_index > -1 and his_clmns:
            db_types_his = [db_types[idx] for idx in his_clmns]
        sql_tohis = sql_inshis
        if sql_tohis.find('${YYYYMM}') >= 0:
            sql_tohis = sql_tohis.replace('${YYYYMM}', time.strftime('%Y%m'))
        while 1:
            rows = self.fetch(cur1, many, lob_index)

            onecount = len(rows)
            if onecount == 0:
                break
            copycount += onecount
            
            try:
                # then insert the target table
                if lob_index > -1: cur2.setinputsizes(*db_types)
                rows1 = []
                send_time=datetime.datetime.now()
                for r in rows:
                    r1 = [i for i in r]
                    r1[time_pos] = send_time
                    read_flag = False
                    if lob_index > -1 and type(r1[lob_index]) == LOB_TYPE:
                        read_flag = True
                        r1[lob_index] = r1[lob_index].read()
                    rows1.append(r1)
                    logger.debug("rowid..: %s read:%s", r1[key_pos[0]], read_flag)
                cur2.executemany(sql_insert, rows1)
                logger.debug("insert rows: %d", cur2.rowcount)
                
                # insert histable
                if his_clmns:
                    hisrows = [] #历史表结构与源表结构不同的情况
                    for r in rows1:
                        hisrows.append([r[idx] for idx in his_clmns])
                    if lob_index > -1: cur3.setinputsizes(*db_types_his)
                else:
                    hisrows = rows1
                    if lob_index > -1: cur3.setinputsizes(*db_types)
                cur3.executemany(sql_tohis, hisrows)
                
                #delete source table
                ids = [ [ r[posa] for posa in key_pos ] for r in rows1]
                cur3.executemany(sql_delete, ids)
            except Exception,ex:
                #失败后，转为一条条处理, 插入失败的记录，修改状态为E,成功的则依然移入历史表
                logger.debug("to_his failed, 1 row %s", ''.join(traceback.format_exception(*sys.exc_info())))
                for row in rows:
                    if lob_index > -1:
                        cur2.setinputsizes(*db_types)
                    try:
                        cur2.execute(sql_insert, row)
                        if his_clmns:
                            #历史表结构与源表结构不同的情况
                            r = [row[idx] for idx in his_clmns]
                            if lob_index > -1: cur3.setinputsizes(*db_types_his)
                        else:
                            r = row
                            if lob_index > -1: cur3.setinputsizes(*db_types)
                        cur3.execute(sql_tohis, r)
                        
                        ids = [ row[posa] for posa in key_pos ]
                        cur3.execute(sql_delete, ids)
                    except Exception,ex:
                        logger.debug("1 row failed, %s", ''.join(traceback.format_exception(*sys.exc_info())))
                        ids = [ row[posa] for posa in key_pos ]
                        print repr(ids)
                        print sql_update_error
                        cur3.execute(sql_update_error, ids)
                        logger.error( "1failed copyone, %s \n %s", repr(ids), ''.join(traceback.format_exception(*sys.exc_info())) )


            self.dbdst.commit()
            self.dbsrc.commit()

        logger.info( "success finished onecopy, %d", copycount)

        cur1.close()
        cur2.close()
        cur3.close()
        return copycount
     
    def run(self):
        while not self.copyinit(): time.sleep(2.0)
        while True:
            ''''t = raw_input('wait(q to exit):')
            if t.strip() == 'q':
                break'''
            count = 0
            try:
                if self.cp_mode:
                    count = self.copyonce_st()
                else:
                    count = self.copyonce_his()
            except:
                # need to reconnect database
                logger.error( "2failed copyone, %s", 
                              ''.join(traceback.format_exception(*sys.exc_info())) )
                self.closeall()
                while not self.copyinit(): time.sleep(2.0)
            if count == 0:
                time.sleep(5.0)
            else:
                time.sleep(1.0)

        self.closeall()
            
def test1():
    #create table src1(id number(12) primary key, status char(1), data clob);
    #create index idx1_src1 on src1(status);
    #create table dst1(id number(12) primary key, status char(1), data clob);
    import cx_Oracle as cx
    co = cx.connect('crm/crm@xe')
    cur = co.cursor()
    cur.execute('truncate table dst1')
    cur.execute('truncate table src1')
    cur.execute('truncate table his1')
    sql = 'insert into src1(id, status, data, tm) values(:v1, :v2, :v3, sysdate)'
    id1 = int(file("id.txt").read().strip())
    clen = int(sys.argv[2])
    rcount = int(sys.argv[1])
    for i in range(rcount):
        id1 += 1
        cur.execute(sql, [id1, '0', '1'*1024*clen])
    co.commit()
    co.close()
    file("id.txt", "w").write(str(id1))
    
if __name__ == '__main__':
    if len(sys.argv) == 2:
        cn = sys.argv[1]
        TableCopy(cn).run()
    else:
        test1()
    
    
