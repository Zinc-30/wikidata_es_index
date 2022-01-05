#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022-01-05 13:16
# @Author  : hxinaa
from search import *

if __name__ == '__main__':
    ws = wikidataSearch(100)
    for x in ws.get_entity_by_id('P31'):
        print(x)
