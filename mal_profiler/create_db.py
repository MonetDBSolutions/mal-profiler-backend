# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import os
import monetdblite


def init_backend(dbpath):
    """Start the profiler backend database and create the tables"""
    monetdblite.init(dbpath)

    # TODO define an abstract root data directory
    cpath = os.path.dirname(os.path.abspath(__file__))
    tables_file = os.path.join(cpath, os.path.join('data', 'tables.sql'))
    with open(tables_file) as sql_fl:
        sql_in = sql_fl.read()

    # Actually create the tables
    for stmt in sql_in.split(';')[:-1]:
        monetdblite.sql(stmt)
