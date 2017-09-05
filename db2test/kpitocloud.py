# -*- coding: utf-8 -*-
import platform,os
import datetime
from ..models import KpiTotalDailyWeb,KpiCompCd005DVerticalWeb
from app import logging
from ..utils import replace_all
import re

class RunMv(object):
    def __init__(self,debugger=False):
        self._pid = '' if platform.system() == 'Windows' else os.getpid()
        self._host = platform.uname()[1]
        self._debugger = debugger

    def mvAll(self):
        KpiCompCd005DVerticalWeb().select().limit(1).echo()
        _dct = KpiCompCd005DVerticalWeb().select().limit(1).query()
        print _dct



