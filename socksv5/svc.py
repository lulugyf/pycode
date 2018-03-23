################################################## ###
import win32serviceutil
import win32service
import win32event

import logging
import logging.handlers

import pm_m as com

ServiceName = "socks_map"

class SmallestPythonService(win32serviceutil.ServiceFramework):
    _svc_name_ = ServiceName
    _svc_display_name_ = ServiceName
    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        
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
