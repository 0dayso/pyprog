# -*- coding: utf-8 -*-

from ..config import ORM_TYPE

if ORM_TYPE=='mysql':
    from orm_mysql import BaseOrm
elif ORM_TYPE=='db2':
    from orm_db2 import BaseOrm