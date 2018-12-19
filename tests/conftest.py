# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018

import json
import os
import pytest

from mal_analytics import profiler_parser
from mal_analytics import db_manager


@pytest.fixture(scope='module')
def query_trace1():
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    fp = open(os.path.join(cur_dir, 'data', 'traces', 'jan2019_sf10_10threads', 'Q01_variation001.json'))
    return [json.loads(x) for x in fp.readlines()]


@pytest.fixture(scope='module')
def query_trace2():
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    fp = open(os.path.join(cur_dir, 'data', 'traces', 'jan2019_sf10_10threads', 'Q02_variation001.json'))
    return [json.loads(x) for x in fp.readlines()]

@pytest.fixture(scope='module')
def supervisor_trace():
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    fp = open(os.path.join(cur_dir, 'data', 'traces', 'distributed', 'supervisor.json'))
    return [json.loads(x) for x in fp.readlines()]

@pytest.fixture(scope='module')
def worker1_trace():
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    fp = open(os.path.join(cur_dir, 'data', 'traces', 'distributed', 'worker1.json'))
    return [json.loads(x) for x in fp.readlines()]

@pytest.fixture(scope='module')
def worker2_trace():
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    fp = open(os.path.join(cur_dir, 'data', 'traces', 'distributed', 'worker2.json'))
    return [json.loads(x) for x in fp.readlines()]

@pytest.fixture(scope='module')
def single_event():
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    fp = open(os.path.join(cur_dir, 'data', 'single_event_formatted.json'))
    return ''.join(fp.readlines())


@pytest.fixture(scope='function')
def parser_object():
    parser = profiler_parser.ProfilerObjectParser()

    return parser

@pytest.fixture(scope='function')
def manager_object(tmp_path):
    db_path = tmp_path.resolve().as_posix()
    manager = db_manager.DatabaseManager(db_path)

    # We got the singleton but we really want a fresh object. Make it so.
    if manager.get_dbpath() != db_path:
        manager.close_connection()
        manager.__class__._instances = {}
        manager = db_manager.DatabaseManager(db_path)

    return manager
