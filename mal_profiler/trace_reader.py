# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import binascii
import bz2
import os
import gzip

from mal_profiler.profiler_parser import ProfilerObjectParser


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
    ln = fl.readline()
    if ln.endswith(u'}\n'):
        return ln.strip()

    buf = [ln]
    while True:
        ln = fl.readline()
        buf.append(ln)
        if ln == '}\n':
            break

    buf.append(ln)
    return ''.join(buf)


def parse_trace(filename, db):
    pob = ProfilerObjectParser(db)
    with abstract_open(filename) as fl:
        pob.parse_object(read_object(fl))
