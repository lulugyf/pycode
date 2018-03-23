# -*- coding: utf-8 -*-

from pexpect import pxssh

#根据bleid的值，
#通过 .ble.list 文件找到对应的ip 和 jolokia 端口
#然后根据 .host.list 找到对应 用户名密码， 登录远程主机
# 执行 grep jmx.jolokiaPort idmm2-broker-[123]/config/ble/server-ble.properties, 找到对应端口数字所在的目录
# 执行目录下的 shutdown.sh
def shutdown_ble(bleid):
    # 10000005 10.162.200.211:15678
    s = file('.ble.list').read()
    p = s.find(bleid); p += len(bleid)+1; p1 = s.find('\n', p);
    host, jolokia_port = s[p:p1].strip().split(':')

    #10.162.200.221  idmm    p3k2!Z[C
    s = file('.host.list').read()
    p = s.find(host); p += len(host)+1; p1 = s.find('\n', p);
    user, pswd = s[p:p1].strip().split()

    print '===', host, user
    try:
        s = pxssh.pxssh()
        s.login(host, user, pswd)

        print('login success')

        s.sendline('grep jmx.jolokiaPort idmm2-broker-[123]/config/ble/server-ble.properties')
        s.prompt()             # match the prompt
        r = s.before           # print everything before the prompt.
        path = ''
        for line in r.split('\n'):
            line = line.strip()
            if line.endswith('=%s'%jolokia_port):
                path = line[0:line.index('/')]
                break
        if path != '':
            #s.sendline('%s/bin/ble/shutdown.sh'%path)
            #s.prompt()
            #print(s.before)
            print '====', '%s/bin/ble/shutdown.sh'%path

        s.logout()
    except pxssh.ExceptionPxssh as e:
        print("pxssh failed on login.")
        print(e)

if __name__ == '__main__':
    shutdown_ble('10000003')
