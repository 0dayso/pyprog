# -*- coding: utf-8 -*-
import platform,os
from app import logging
from .kpitocloud import *
from time import sleep

class Pyprog(object):
    def __init__(self,**kwargs):
        #初始化参数
        self._runtype = None
        self._gbascode = None
        self._taskid = None
        self._debugger = False
        for i in kwargs.keys():
            if i=='z':self._gbascode = None if not kwargs['z'] else replace_all(kwargs['z'])
            elif i=='t':self._taskid= None if not kwargs['t'] else kwargs['t'].strip()
            elif i=='runtype':self._runtype = None if not kwargs['runtype'] else kwargs['runtype'].strip()
            elif i=='debugger':self._debugger = True if kwargs['debugger']==True else False

        if self._debugger == True: logging._debugger = True
        self._pid = '' if platform.system() == 'Windows' else os.getpid()
        self._host = platform.uname()[1]

    def go(self):
        if self._runtype == 'kpitocloud':
            logging.Info("启动kpitocloud守护模式,进程号%s,主机名%s" % (self._pid,self._host))
            while 1==1:
                _re = RunMv(debugger=self._debugger).mvAll()
                if _re:
                    pass
                else:
                    sleep(60)
        