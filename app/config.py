# -*- coding: utf-8 -*-
import platform

#pro
if platform.uname()[1] in ('hbngdb1','hbdwdb2'):
    ORM_TYPE='db2'
    LOG_DIR='/expmonth/log/ipd/log/'
    DB_CONFIG={
        'default':{
            'db_type' : 'db2',
            'db_host' : '10.25.124.221',
            'db_port' : '50000',
            'db_name' : 'dwdb',
            'db_user' : 'ipd',
            'db_password': 'mmxg!0103'}
    }
    RUNCALC_SLEEP=300
    RUNEXP_SLEEP=300
    WARN_SLEEP=30
#dacp
elif platform.uname()[1] in ('AIBDC-WEB-01'):
    ORM_TYPE='db2'
    LOG_DIR='/home/dacp/app/log/'
    DB_CONFIG={
        'default':{
            'db_type' : 'db2',
            'db_host' : '192.168.1.200',
            'db_port' : '50000',
            'db_name' : 'dwdb',
            'db_user' : 'db2inst1',
            'db_password': '123123'}
    }
    RUNCALC_SLEEP=300
    RUNEXP_SLEEP=300
    WARN_SLEEP=30
#fust
elif platform.uname()[1] in ('DESKTOP-MB2R9K5'):
    ORM_TYPE='mysql'
    LOG_DIR='D:\gitproject'
    DB_CONFIG={
        'default':{
            'db_type' : 'mysql',
            'db_host' : '127.0.0.1',
            'db_port' : '3306',
            'db_name' : 'gbas',
            'db_user' : 'root',
            'db_password': 'root'}
    }
    RUNCALC_SLEEP=300
    RUNEXP_SLEEP=300
    WARN_SLEEP=300
#bs dacp-dev1
elif platform.uname()[1] in ('dacp-dev1'):
    ORM_TYPE='mysql'
    LOG_DIR='/home/dacp/app/log/'
    DB_CONFIG={
        'default':{
            'db_type' : 'mysql',
            'db_host' : '127.0.0.1',
            'db_port' : '3306',
            'db_name' : 'gbas',
            'db_user' : 'root',
            'db_password': 'root'}
    }
    RUNCALC_SLEEP=300
    RUNEXP_SLEEP=300
    WARN_SLEEP=300
else:
    ORM_TYPE='mysql'
    LOG_DIR='/Users/xiaoqi/gitproject/'
    DB_CONFIG={
        'default':{
            'db_type' : 'mysql',
            'db_host' : '127.0.0.1',
            'db_port' : '3306',
            'db_name' : 'gbas',
            'db_user' : 'root',
            'db_password': 'root'}
    }
    RUNCALC_SLEEP=300
    RUNEXP_SLEEP=300
    WARN_SLEEP=30