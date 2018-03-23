################################################## ###
import win32serviceutil
import win32service
import win32event

import logging
import logging.handlers

import pm_m as com

ServiceName = "socks_map"

''' python svc.py 
Usage: 'svc.py [options] install|update|remove|start [...]|stop|restart [...]|de
bug [...]'
Options for 'install' and 'update' commands only:
 --username domain\username : The Username the service is to run under
 --password password : The password for the username
 --startup [manual|auto|disabled|delayed] : How the service starts, default = ma
nual
 --interactive : Allow the service to interact with the desktop.
 --perfmonini file: .ini file to use for registering performance monitor data
 --perfmondll file: .dll file to use when querying the service for
   performance data, default = perfmondata.dll
Options for 'start' and 'stop' commands only:
 --wait seconds: Wait for the service to actually start or stop.
                 If you specify --wait with the 'stop' option, the service
                 and all dependent services will be stopped, each waiting
                 the specified period.
python svc.py --startup auto install
python svc.py start   or  net start socks_map
'''

class SmallestPythonService(win32serviceutil.ServiceFramework):
    _svc_name_ = ServiceName
    _svc_display_name_ = ServiceName
    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        
        #logging.basicConfig(
        #        filename='e:/temp/svc.log',
        #        level=logging.DEBUG,
        #        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh = logging.handlers.TimedRotatingFileHandler("C:/log/%s.log"%ServiceName, when="D")
        fmtstr = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        formatter = logging.Formatter(fmtstr)
        fh.setFormatter(formatter)

        log = logging.getLogger(ServiceName)
        log.addHandler(fh)
        log.setLevel(logging.DEBUG)
        com.log = log

    def SvcStop(self):
        # Before we do anything, tell the SCM we are starting the stop process.
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        # And set my event.
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        # We do nothing other than wait to be stopped!
        svc1 = com.Service()
        svc1.start()
        #svc2.start()
        win32event.WaitForSingleObject(self.hWaitStop, win32event.INFINITE)
        svc1.stop()
        #svc2.stop()

if __name__=='__main__':
    win32serviceutil.HandleCommandLine(SmallestPythonService)
