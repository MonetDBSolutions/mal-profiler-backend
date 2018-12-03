# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018

import os

import pytest

from mal_analytics import profiler_parser
from mal_analytics import db_manager


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
def parser_object():
    parser = profiler_parser.ProfilerObjectParser()

    return parser


@pytest.fixture(scope='function')
def manager_object(tmp_path):
    db_path = tmp_path.resolve().as_posix()
    manager = db_manager.DatabaseManager(db_path)

    return manager
