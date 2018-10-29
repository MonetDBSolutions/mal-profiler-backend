# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import tempfile
import pytest
from mal_profiler import create_db


@pytest.fixture(scope='function')
def backend():
    """Initialize the backend"""
    db_path = tempfile.mkdtemp(suffix='_mdbl')
    connection = create_db.init_backend(db_path)

    yield (db_path, connection)
    connection.close()


def test_initialization(backend):
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
    test_initialization((backend[0], connection))
    connection.close()
