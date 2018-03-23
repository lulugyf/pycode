import pywinauto

pwa_app = pywinauto.application.Application()
w_handle = pywinauto.findwindows.find_windows(title=u'Piriform recuva')[0]
window = pwa_app.window_(handle=w_handle)
ctrl = window['TreeView']  #SysTreeView32
ctrl.GetItem([u'Connection']).Click()  #Click on a top level element
ctrl.GetItem([u'Connection', u'Data']).Click()  #Click on a sub element