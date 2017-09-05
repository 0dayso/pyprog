# -*- coding: utf-8 -*-
import platform,os
import datetime
from ..models import KpiTotalDailyWeb
from app import logging
from ..utils import replace_all
import re

class RunMv(object):
    def __init__(self,debugger=False):
        self._pid = '' if platform.system() == 'Windows' else os.getpid()
        self._host = platform.uname()[1]
        self._debugger = debugger

    def mvAll(self):
        _dct = KpiTotalDailyWeb().select().limit(1).query()


    
