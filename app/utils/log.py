# -*- coding: utf-8 -*-
# import fcntl
import os
import datetime
from ..config import LOG_DIR

class WriteLog(object):
    _taskname=None
    def __init__ (self,debugger=False):
        self._LogDir=LOG_DIR
        self._LogTaskId = None
        self._debugger = debugger

    def Info(self,Text):
        self._LogTaskId = datetime.datetime.now().strftime('%Y%m%d')
        with open(("%sGbas_%s.log" % (self._LogDir,self._LogTaskId)),"a+",0) as f:
            # fcntl.flock(f, fcntl.LOCK_EX)
            if self._taskname==None:
                _msg = (datetime.datetime.now().strftime('Info [%Y-%m-%d %H:%M:%S] :') + Text).replace(os.linesep,'  ')
            else:
                _msg = (datetime.datetime.now().strftime('Info [%Y-%m-%d %H:%M:%S]') + ' %s:' % self._taskname + Text).replace(os.linesep,'  ')
            print(_msg)
            f.write(_msg + os.linesep)

    def Debug(self,Text,is_horizontal=True):
        if self._debugger == False: pass
        else:
            self._LogTaskId = datetime.datetime.now().strftime('%Y%m%d')
            with open(("%sGbas_%s.log" % (self._LogDir,self._LogTaskId)),"a+",0) as f:
                # fcntl.flock(f, fcntl.LOCK_EX)
                if is_horizontal==True:
                    if self._taskname==None:
                        _msg = (datetime.datetime.now().strftime('Debug[%Y-%m-%d %H:%M:%S] :') + Text).replace(os.linesep,'  ')
                    else:
                        _msg = (datetime.datetime.now().strftime('Debug[%Y-%m-%d %H:%M:%S]') + ' %s:' % self._taskname + Text).replace(os.linesep,'  ')
                else:
                    if self._taskname==None:
                        _msg = (datetime.datetime.now().strftime('Debug[%Y-%m-%d %H:%M:%S] :') + Text)
                    else:
                        _msg = (datetime.datetime.now().strftime('Debug[%Y-%m-%d %H:%M:%S]') + ' %s:' % self._taskname + Text)
                print(_msg)
                f.write(_msg + os.linesep)

    def Alarm(self,Text):
        self._LogTaskId = datetime.datetime.now().strftime('%Y%m%d')
        with open(("%sGbas_%s.log" % (self._LogDir,self._LogTaskId)),"a+",0) as f:
            # fcntl.flock(f, fcntl.LOCK_EX)
            if self._taskname==None:
                _msg = (datetime.datetime.now().strftime('Alarm[%Y-%m-%d %H:%M:%S] :') + Text).replace(os.linesep,'  ')
            else:
                _msg = (datetime.datetime.now().strftime('Alarm[%Y-%m-%d %H:%M:%S]') + ' %s:' % self._taskname + Text).replace(os.linesep,'  ')
            print(_msg)
            f.write(_msg + os.linesep)