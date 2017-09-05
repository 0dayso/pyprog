# -*- coding: utf-8 -*-
import platform,os
from app import logging
from .comm import *
from .dispatch import *
from .runcalc import *
from .runexp import *
from ..bwarn import *
from ..utils import replace_all
from ..config import RUNCALC_SLEEP,RUNEXP_SLEEP,WARN_SLEEP
from time import sleep

class Gbas(object):
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
        if self._runtype == 'dispatch':
            chk_taskid(self._taskid)
            logging.Info("启动dispatch分发模式,批次%s" % self._taskid)
            Dispatch(taskid=self._taskid,gbascode=self._gbascode,debugger=self._debugger).disAll()
        elif self._runtype == 'diszb':
            chk_taskid(self._taskid)
            logging.Info("启动diszb分发模式,批次%s" % self._taskid)
            Dispatch(taskid=self._taskid,gbascode=self._gbascode,debugger=self._debugger).disZb()
        elif self._runtype == 'disex':
            chk_taskid(self._taskid)
            logging.Info("启动disex分发模式,批次%s" % self._taskid)
            Dispatch(taskid=self._taskid,gbascode=self._gbascode,debugger=self._debugger).disEx()
        elif self._runtype == 'runcalc':
            logging.Info("启动runcalc守护模式,进程号%s,主机名%s" % (self._pid,self._host))
            while 1==1:
                _id = Runcalc(debugger=self._debugger).getCalcTask()
                if _id == 0:
                    logging._taskname=None
                    logging.Info("runcalc守护模式进入等待状态,等待%s秒,进程号%s,主机名%s" % (RUNCALC_SLEEP,self._pid,self._host))
                    sleep(RUNCALC_SLEEP)
                else:
                    # Runcalc(debugger=self._debugger).calcAll(_id)
                    try:
                        Runcalc(debugger=self._debugger).calcAll(_id)
                    except Exception, ex:
                        logging.Alarm("runcalc守护模式执行失败,进程号:%s,主机名:%s,异常信息:%s" % (self._pid,self._host,ex))
                        Runcalc(debugger=self._debugger).calcErrDel(_id,str(ex))
        elif self._runtype == 'runexp':
            logging.Info("启动runexp守护模式,进程号%s,主机名%s" % (self._pid,self._host))
            while 1==1:
                _id = Runexp(debugger=self._debugger).getExpTask()
                if _id == 0:
                    logging._taskname=None
                    logging.Info("runexp守护模式进入等待状态,等待%s秒,进程号%s,主机名%s" % (RUNEXP_SLEEP,self._pid,self._host))
                    sleep(RUNEXP_SLEEP)
                else:
                    # Runexp(debugger=self._debugger).expAll(_id)
                    try:
                        Runexp(debugger=self._debugger).expAll(_id)
                    except Exception, ex:
                        logging.Alarm("runexp守护模式执行失败,进程号:%s,主机名:%s,异常信息:%s" % (self._pid,self._host,ex))
                        Runcalc(debugger=self._debugger).expErrDel(_id,str(ex))
        elif self._runtype == 'warn':
            logging.Info("启动warn守护模式,进程号%s,主机名%s" % (self._pid,self._host))
            while 1==1:
                Warner(debugger=self._debugger).generateWarnTask()
                Warner(debugger=self._debugger).warnAll()
                logging.Info("warn守护模式进入等待状态,等待%s秒,进程号%s,主机名%s" % (WARN_SLEEP,self._pid,self._host))
                sleep(WARN_SLEEP)



