# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018

import os
import shutil

import pytest
import monetdblite

from mal_profiler import create_db
from mal_profiler import profiler_parser


@pytest.fixture(scope='function')
def backend(request, tmp_path):
    """Initialize the backend"""
    db_path = tmp_path.resolve().as_posix()
    # print("Initializind directory: {}".format(db_path))

    def finalizer():
        connection.close()
        monetdblite.shutdown()
        if tmp_path.is_dir():
            shutil.rmtree(db_path)

    request.addfinalizer(finalizer)
    connection = create_db.init_backend(db_path)

    return (db_path, connection)


@pytest.fixture(scope='module')
def query_trace():
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    fp = open(os.path.join(cur_dir, 'data', 'Q01_variation100.json'))
    return fp.readlines()


@pytest.fixture(scope='module')
def query_trace2():
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    fp = open(os.path.join(cur_dir, 'data', 'Q02_variation100.json'))
    return fp.readlines()


@pytest.fixture(scope='module')
def single_event():
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    fp = open(os.path.join(cur_dir, 'data', 'single_event_formatted.json'))
    return ''.join(fp.readlines())


@pytest.fixture(scope='function')
def create_monetdb_connection():
    yield None


@pytest.fixture(scope='function')
def parser_object(backend):
    parser = profiler_parser.ProfilerObjectParser(backend[1])

    return parser
