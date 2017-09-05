# -*- coding: utf-8 -*-
from __future__ import division
import sys,datetime
reload(sys)
sys.setdefaultencoding('utf-8')

from .db import InitPoolDB
from .config import DB_CONFIG
from .utils import WriteLog


logging = WriteLog()

PoolDB = InitPoolDB(DB_CONFIG)
