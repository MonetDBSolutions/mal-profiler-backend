# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import os
import monetdblite


def init_backend(dbpath):
    """Start the profiler backend database and create the tables"""
    connection = monetdblite.make_connection(dbpath, autocommit=True)

    # TODO define an abstract root data directory
    cpath = os.path.dirname(os.path.abspath(__file__))
    tables_file = os.path.join(cpath, 'data', 'tables.sql')
    execute_sql_script(connection, tables_file)

    return connection


def start_backend(dbpath):
    connection = monetdblite.make_connection(dbpath, autocommit=True)

    return connection


def execute_sql_script(connection, script_path):
    with open(script_path) as sql_fl:
        sql_in = sql_fl.read()

    for stmt in sql_in.split(';')[:-1]:
        connection.execute(stmt)
