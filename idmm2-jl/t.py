
from idmm2_client import getBrokerList, DMMClient

'''
src_topic_id: TRecOprCntt
dst_topic_id: TRecOprCnttDest

producer_id:  Pub101
consumer_id:  Sub119Opr
'''
def produce_consume():
    #print getBrokerList('127.0.0.1:2181', '/idmm2/httpbroker')

    src_topic_id = 'TRecOprCntt'
    producer_id = 'Pub101'

    dst_topic_id = 'TRecOprCnttDest'
    consumer_id = 'Sub119Opr'
    addr = '127.0.0.1:61717'

    c = DMMClient(addr)
    msgid = c.send(src_topic_id, producer_id, u'hello world')
    if msgid is None:
        return False
    print 'message id=%s' %msgid
    c.send_commit(src_topic_id, producer_id, msgid, 10, '13900')

    # consume
    while 1:
        msgid, content = c.fetch(dst_topic_id, consumer_id)
        if msgid == 0:
            print 'no more message'
            break
        elif msgid is not None:
            print '======got', msgid, content
            c.fetch_commit(dst_topic_id, consumer_id, msgid,
                           'commit description:'+msgid, 'notice_1')
        else:
            print 'fetch failed'
            break

def notice_recv():
    dst_topic_id = 'notice_1'
    consumer_id = 'notice_sub_1'
    addr = '127.0.0.1:53260'

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
    produce_consume()
    #notice_recv()
