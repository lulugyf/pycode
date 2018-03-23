
from idmm2_client import getBrokerList, DMMClient

'''
src_topic_id: TRecOprCntt
dst_topic_id: TRecOprCnttDest

producer_id:  Pub101
consumer_id:  Sub119Opr
'''
def produce_consume():
    #print getBrokerList('172.21.0.46:4621', '/idmm2/httpbroker')

    src_topic_id = 'Test'
    producer_id = 'pub_Test'

    dst_topic_id = 'Test'
    consumer_id = 'sub_Test'
    addr = '172.21.0.46:4621'

    c = DMMClient(addr)
    msgid = c.send(src_topic_id, producer_id, u'hello world')
    if msgid is None:
        return False
    print 'message id=%s' %msgid
    c.send_commit(src_topic_id, producer_id, msgid, 10, '13900')

    # consume
    last_msgid = None
    pull_code = None
    desc = 'code-description'
    while 1:
        j = c.pull(dst_topic_id, consumer_id, 60,
                                last_msgid, pull_code, desc)
        rcode = j['result-code']
        if rcode == 'NO_MORE_MESSAGE':
            print 'no more message'
            break
        elif rcode == 'OK':
            msgid = j['message-id']
            content = j['content']
            print '======got', msgid, content
            pull_code = 'ROLLBACK_AND_NEXT'
            last_msgid = msgid
            desc = 'rollback and go to hell'
        else:
            print 'fetch failed'
            break

def notice_recv():
    dst_topic_id = 'notice_1'
    consumer_id = 'notice_sub_1'
    addr = '172.21.0.46:4621'

    c = DMMClient(addr)
    # consume
    while 1:
        msgid, content = c.fetch(dst_topic_id, consumer_id)
        if msgid == 0:
            print 'no more message'
            break
        elif msgid is not None:
            print '======got', msgid, content
            c.fetch_commit(dst_topic_id, consumer_id, msgid)
        else:
            print 'fetch failed'
            break
    
if __name__ == '__main__':
    #print getBrokerList("172.21.0.46:4621")
    produce_consume()
    #notice_recv()
    pass
