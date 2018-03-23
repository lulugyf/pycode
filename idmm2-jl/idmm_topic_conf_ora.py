#coding=utf-8

import cx_Oracle

# select table_name from all_tables where owner='DBIDMMOPR';
def __conndb__():
    #co = cx_Oracle.connect('idmm/ll@xe')
    co = cx_Oracle.connect('dbidmmopr/dbidmmopr@idmmdb')
    cur = co.cursor()
    return co, cur

def idx_tables(ni=10, nm=None):
    if nm is None:
        nm = ni

    sql = '''CREATE TABLE %s (
  idmm_msg_id varchar(60) NOT NULL,
  produce_cli_id varchar(32) NULL,
  src_topic_id varchar(32) NULL,
  dst_cli_id varchar(32) NOT NULL,
  dst_topic_id varchar(32) NOT NULL,
  src_commit_code varchar(4) NULL,
  group_id varchar(32) NULL,
  priority number(11) NOT NULL ,
  idmm_resend number(11) NULL,
  consumer_resend number(11) NULL,
  create_time number(20) NULL,
  broker_id varchar(21) NULL,
  req_time number(20) NULL,
  commit_code varchar(4) NULL,
  commit_time number(20) NULL,
  commit_desc varchar(1024) NULL,
  next_topic_id varchar(32) NULL,
  next_client_id varchar(32) NULL,
  expire_time number(20),
  PRIMARY KEY (idmm_msg_id,dst_cli_id,dst_topic_id)
)'''
    sql_idx = 'create Index %s_idx on %s(dst_cli_id,dst_topic_id)'

    sql_store = '''CREATE TABLE messagestore_%d (
  id varchar(128) NOT NULL,
  properties varchar(2048) NULL,
  systemProperties varchar(1024) NULL,
  content blob,
  createtime number(20) NULL,
  PRIMARY KEY (id)
)'''


    co, cur = __conndb__()

    for i in range(ni):
        print i
        table_name = 'msgidx_part_%d'%i
        try:
            cur.execute("drop table %s"%table_name) # drop table
        except: pass
        sql1 = sql % table_name
        cur.execute(sql1)  # create table

        sql1 = sql_idx %(table_name, table_name)
        cur.execute(sql1) # create index

        #table_name = "msgidx_part_his_%d" % i
        #try:
        #    cur.execute("drop table %s" % table_name) # drop history table
        #except: pass
        #cur.execute(sql % table_name)  # create table of history
    for i in range(nm):
        try:
            cur.execute("drop table messagestore_%d" % i)
        except: pass
        cur.execute(sql_store % i)
    cur.close()
    co.close()


def clear_tables():
    co, cur = __conndb__()
    print "clear msgidx_part_{i} ..."
    for i in range(10):
        print i
        cur.execute("truncate table msgidx_part_%d" % i)

    print "clear messagestore_{i} ..."
    for i in range(10):
        print i
        cur.execute("truncate table messagestore_%d" % i)

    cur.close()
    co.close()

    print "done"

'''
zkCli.sh -server 172.21.11.63:2180
create /idmm 0 0 0
create /idmm/configServer 0 0 0
create /idmm/configServer/version 8 0 0
'''
class Conf:
    def __init__(self, confVer):
        self.confVer = confVer
        self.co, self.cur = __conndb__()
    def close(self):
        self.cur.close()
        self.co.close()

    def add_topic(self, bleId, pubId, srcTopic, subId, dstTopic):
        co, cur = self.co, self.cur
        confVer = self.confVer
        try:
            # 客户端id 资料添加
            try:
                cur.execute("insert into tc_client_%s values(:v1, 'sub_system', 'client_desc', '1', 'login_no', sysdate, 'note')" % confVer,
                        (pubId,))
            except cx_Oracle.IntegrityError,e:
                pass # 忽略重复的client_id
            try:
                cur.execute("insert into tc_client_%s values(:v1, 'sub_system', 'client_desc', '1', 'login_no', sysdate, 'note')" % confVer,
                        (subId,))
            except cx_Oracle.IntegrityError,e:
                pass # 忽略重复的client_id

            # 源主题资料表
            try:
                cur.execute("insert into tc_src_topic_%s values(:v1, 'src_topic_desc', '1', 'login_no', sysdate, 'note')" % confVer,
                        (srcTopic, ))
            except cx_Oracle.IntegrityError,e:
                pass # 忽略重复的 src_topic

            # 目标主题资料表
            try:
                cur.execute("insert into tc_dest_topic_%s values(:v1, 'dest_topic_desc', '1', 'login_no', sysdate, 'note')" % confVer,
                    (dstTopic,))
            except cx_Oracle.IntegrityError,e:
                pass # 忽略重复的 dest_topic

            #源主题到目标主题映射
            try:
                cur.execute("insert into tc_topic_map_%s values(:v1, '_all', '_default', :v2, '1', 'login_no', sysdate, 'note')" % confVer,
                    (srcTopic, dstTopic,))
            except cx_Oracle.IntegrityError,e:
                pass

            # bleid 与 目标主题的映射
            try:
                cur.execute("insert into tc_ble_dest_topic_%s values(:v1, :v2, '1', 'login_no', sysdate, 'note')" % confVer,
                    (dstTopic, bleId))
            except cx_Oracle.IntegrityError,e:
                pass

            # 发布关系表
            try:
                cur.execute("insert into tc_topic_pub_%s values(:v1, :v2, '_null', '1', 'login_no', sysdate, 'note')" % confVer,
                        (pubId, srcTopic))
            except cx_Oracle.IntegrityError,e:
                pass

            # 订阅关系表
            # client_id, dest_topic_id, client_pswd, max_request, min_timeout, max_timeout, use_status, login_no, opr_time,
            #  note, consume_speed_limit, max_messages, warn_messages
            try:
                cur.execute("insert into tc_topic_sub_%s values(:v1, :v2, '_null', 20, 60, 600, '1', 'login_no', sysdate, "
                        "'note', 0, 30000, 1000)" % confVer,
                        (subId, dstTopic))
            except cx_Oracle.IntegrityError,e:
                pass

            co.commit()
        except str,e:
            co.rollback()
            raise e

    def clear_topics(self):
        confVer = self.confVer
        co, cur = self.co, self.cur
        cur.execute('truncate table tc_client_%s' % confVer)
        cur.execute('truncate table tc_src_topic_%s' % confVer)
        cur.execute('truncate table tc_dest_topic_%s' % confVer)
        cur.execute('truncate table tc_topic_map_%s' % confVer)
        cur.execute('truncate table tc_ble_dest_topic_%s' % confVer)
        cur.execute('truncate table tc_topic_pub_%s' % confVer)
        cur.execute('truncate table tc_topic_sub_%s' % confVer)

def conf():
    c = Conf("8")
    c.clear_topics()
    # c.add_topic('10000001', 'pubTest1', 'TSrcTest1', 'subTest1', 'TDstTest1')
    # c.add_topic('10000002', 'pubTest2', 'TSrcTest2', 'subTest2', 'TDstTest2')
    # c.add_topic('10000003', 'pubTest3', 'TSrcTest3', 'subTest3', 'TDstTest3')
    # c.add_topic('10000004', 'pubTest4', 'TSrcTest4', 'subTest4', 'TDstTest4')
    # c.add_topic('10000005', 'pubTest5', 'TSrcTest5', 'subTest5', 'TDstTest5')
    # c.add_topic('10000006', 'pubTest6', 'TSrcTest6', 'subTest6', 'TDstTest6')
    i = 0
    ble = ('10000001','10000002','10000003','10000004','10000005','10000006')
    for line in file('topics.txt'):
        pubid, srctopic, subid, dsttopic = line.strip().split()
        print '---', pubid, srctopic
        c.add_topic(ble[i%len(ble)], pubid, srctopic+'-A', subid, dsttopic+'-A')
        c.add_topic(ble[i%len(ble)], pubid, srctopic+'-B', subid, dsttopic+'-B')
        i += 1
    c.close()


if __name__ == '__main__':
    #idx_tables(100, 120)
    conf()
    #clear_tables()
