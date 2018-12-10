# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018

import binascii
import bz2
import gzip
import json
import logging
import os

from mal_analytics.profiler_parser import ProfilerObjectParser
from mal_analytics.db_manager import DatabaseManager

LOGGER = logging.getLogger(__name__)


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


def read_object(fl):
    buf = []
    for ln in fl:
        buf.append(ln)
        if ln.endswith(u'}\n'):
            json_string = ''.join(buf).strip()
            return json_string
            # print(json_string)

def parse_trace(filename, database_path):
    dbm = DatabaseManager(database_path)

    cpath = os.path.dirname(os.path.abspath(__file__))
    drop_constraints_script = os.path.join(cpath, 'data', 'drop_constraints.sql')
    dbm.execute_sql_script(drop_constraints_script)

    pob = dbm.create_parser()

    with abstract_open(filename) as fl:
        LOGGER.debug("Parsing trace from file %s", filename)

        json_stream = list()
        json_string = read_object(fl)
        while json_string:
            json_stream.append(json.loads(json_string))
            json_string = read_object(fl)


    pob.parse_trace_stream(json_stream)
    for table, data in pob.get_data().items():
        # print("Inserting data in", table)
        #for k, v in data.items():
            # print("  ", k, " => ", len(v))
        dbm.insert_data(table, data)

    pob.clear_internal_state()
    add_constraints_script = os.path.join(cpath, 'data', 'add_constraints.sql')
    dbm.execute_sql_script(add_constraints_script)
