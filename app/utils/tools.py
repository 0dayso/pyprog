# -*- coding: utf-8 -*-

def replace_all(text, rep=['\r','\n',' '], res=''):
    """replace_all('(123){456}','(){}')"""
    for i in rep:
        text = text.replace(i, res)
    return text

