# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018

import binascii
import bz2
import os
import gzip

from mal_analytics.profiler_parser import ProfilerObjectParser
from mal_analytics.create_db import execute_sql_script


def test_gzip(filename):
    '''Checks the if the first two bytes of the file match the gzip magic number'''
    with open(filename, 'rb') as ff:
        return binascii.hexlify(ff.read(2)) == b'1f8b'


def test_bz2(filename):
    '''Checks the if the first two bytes of the file match the bz2 magic number'''
    with open(filename, 'rb') as ff:
        return binascii.hexlify(ff.read(2)) == b'425a'


def abstract_open(filename):
    '''Open a file for reading, automatically detecting a number of compression schemes
    '''
    compressions = {
        test_gzip: gzip.open,
        test_bz2: bz2.open
    }

    for tst, fcn in compressions.items():
        if tst(filename):
            return fcn(filename, 'rt', encoding='utf-8')

    return open(filename, 'r')


# def parse_trace(filename, connection):
#     cpath = os.path.dirname(os.path.abspath(__file__))
#     drop_constraints_script = os.path.join(cpath, 'data', 'drop_constraints.sql')
#     execute_sql_script(connection, drop_constraints_script)

#     pob = ProfilerObjectParser(connection)
#     with abstract_open(filename) as fl:
#         buf = []
#         for ln in fl:
#             buf.append(ln)
#             if ln.endswith(u'}\n'):
#                 json_string = ''.join(buf).strip()
#                 buf = []
#                 # print(json_string)
#                 pob.parse_object(json_string)

#     add_constraints_script = os.path.join(cpath, 'data', 'add_constraints.sql')
#     execute_sql_script(connection, add_constraints_script)
