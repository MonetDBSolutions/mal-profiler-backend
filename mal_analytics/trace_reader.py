# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018-2019

import binascii
import bz2
import gzip
import json
import logging

from mal_analytics.db_manager import DatabaseManager
from mal_analytics import exceptions

LOGGER = logging.getLogger(__name__)


def is_gzip(filename):
    '''Checks the if the first two bytes of the file match the gzip magic number'''
    with open(filename, 'rb') as ff:
        return binascii.hexlify(ff.read(2)) == b'1f8b'


def is_bzip2(filename):
    '''Checks the if the first two bytes of the file match the bz2 magic number'''
    with open(filename, 'rb') as ff:
        return binascii.hexlify(ff.read(2)) == b'425a'


def abstract_open(filename):
    '''Open a file for reading, automatically detecting a number of compression schemes
    '''
    compressions = {
        is_gzip: gzip.open,
        is_bzip2: bz2.open
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

def parse_trace(filename, database_path):  # pragma: no coverage
    dbm = DatabaseManager(database_path)
    pob = dbm.create_parser()

    with abstract_open(filename) as fl:
        LOGGER.debug("Parsing trace from file %s", filename)

        json_stream = list()
        json_string = read_object(fl)
        while json_string:
            try:
                json_stream.append(json.loads(json_string))
            except Exception as e:
                LOGGER.error("JSON parser failed for file %s:\n %s", filename, e)
                raise
            json_string = read_object(fl)


    pob.parse_trace_stream(json_stream)
    dbm.transaction()
    try:
        dbm.drop_constraints()
        for table, data in pob.get_data().items():
            dbm.insert_data(table, data)
        dbm.add_constraints()
    except exceptions.AnalyticsException as e:
        LOGGER.error(e)
        dbm.rollback()
    dbm.commit()
    pob.clear_internal_state()
