# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018

import os

from mal_profiler import create_db


def test_table_creation(backend):
    '''Make sure that all the tables get created correctly'''

    cursor = backend[1].cursor()
    tables = [
        'mal_execution',
        'profiler_event',
        'prerequisite_events',
        'mal_type',
        'mal_variable',
        'return_variable_list',
        'argument_variable_list',
        'heartbeat',
        'cpuload'
    ]
    for tbl in tables:
        rslt = cursor.execute("SELECT id FROM _tables WHERE name =%s", tbl)
        assert rslt == 1

    cursor.close()


def test_restart(backend):
    backend[1].close()
    connection = create_db.start_backend(backend[0])
    test_table_creation((backend[0], connection))
    connection.close()


def test_sql_execution(backend):
    spath = os.path.dirname(os.path.abspath(__file__))
    create_db.execute_sql_script(backend[1], os.path.join(spath, 'data', 'test_sql_script.sql'))
    cursor = backend[1].cursor()

    rslt = cursor.execute("SELECT tag FROM mal_execution")
    assert rslt == 6
    rslt = cursor.execute("SELECT tag FROM mal_execution WHERE server_session='a11bd16e-dc2d-11e8-aa5e-e4b318554ad8'")
    assert rslt == 3
