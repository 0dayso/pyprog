#!/usr/bin/env python
#coding: utf-8

import optparse
from app.controller import Gbas

if __name__ == "__main__":
    """runtype:dispatch:发布,runzb:指标运算(守护模式,可多进程),runrule:规则运算(守护模式,可多进程)"""
    # Gbas(runtype='dispatch',t='20170726',z='D02001Z0001; D02001Z0002; D02001Z0003',debugger=True).go()
    # Gbas(runtype='runcalc',debugger=True).go()
    # Gbas(runtype='dispatch',t='20170727',debugger=True).go()
    # Gbas(runtype='runcalc',debugger=True).go()
    # Gbas(runtype='runexp',debugger=True).go()
    Gbas(runtype='warn',debugger=True).go()

