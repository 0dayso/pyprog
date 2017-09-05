#!/usr/bin/env python
#coding: utf-8

import optparse
from app.controller import Pyprog

if __name__ == "__main__":
    Pyprog(runtype='kpitocloud').go()

