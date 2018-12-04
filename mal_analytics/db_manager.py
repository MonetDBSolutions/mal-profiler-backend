# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018

import logging
import os
import monetdblite

from mal_analytics.exceptions import InitializationError
from mal_analytics.profiler_parser import ProfilerObjectParser

LOGGER = logging.getLogger(__name__)


class Singleton(type):
    """Singleton pattern implementation for Python 3.

See also `this <https://stackoverflow.com/a/6798042>`_
stackoverflow post.
"""
    # We should consider having one instance per data directory,
    # although it probably does not make sense with MonetDBLite.
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)

        obj = cls._instances[cls]
        if not obj.is_connected():
            obj._connect()

        return obj


class DatabaseManager(object, metaclass=Singleton):
    """A connection manager for the database.

This class is a *signleton*. Singletons are to be avoided in
general, but unfortunatelly any component communicating directly
with MonetDBLite should be a singleton, because of the way
MonetDBLite operates.


:param dbpath: The directory to initialize MonetDBLite
"""

    def __init__(self, dbpath):
        self._dbpath = dbpath
        self._connection = None
        self._connect()
        self._initialize_tables()

    def _connect(self):
        self._connection = monetdblite.make_connection(self._dbpath, True)

    def _initialize_tables(self):
        # TODO define an abstract root data directory
        cpath = os.path.dirname(os.path.abspath(__file__))
        tables_file = os.path.join(cpath, 'data', 'tables.sql')
        try:
            self.execute_sql_script(tables_file)
        except monetdblite.Error as e:
            LOGGER.warning("Table initialization script failed:\n  %s", e)
            self._connection.rollback()

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
        cursor = self.get_cursor()
        for tbl in tables:
            rslt = cursor.execute("SELECT id FROM _tables WHERE name =%s", tbl)
            # This should never happen
            if rslt != 1:
                raise InitializationError("Did not find table %s in database %s", tbl, self.get_dbpath)

    def get_dbpath(self):
        """Get the location on disk of the database.

:returns: The path where the database has been initialized.
"""
        return self._dbpath

    def execute_query(self, query):
        """Execute a single query and return the results.

:param query: The text of the query.
:returns: The results
"""
        cursor = self._connection.cursor()
        try:
            cursor.execute(query)
            results = cursor.fetchall()
        except monetdblite.Error as e:
            LOGGER.warning("query\n  %s\n failed with message: %s", query, e)
            results = None

        return results

    def execute_sql_script(self, script_path):
        """Execute a given sql script.

This method does not return results. This is intended for
executing scripts with side effects, i.e. table creation, data
loading, and adding/dropping constraints.

:param script_path: A string with the path to the script.
"""
        with open(script_path) as sql_fl:
            sql_in = sql_fl.readlines()

        stmt = list()
        for ln in sql_in:
            cline = ln.strip()
            if ln.startswith('--') or len(cline) == 0:
                continue

            stmt.append(cline)
            if cline.endswith(';'):
                statement = " ".join(stmt)
                print(statement)
                self._connection.execute(statement)
                stmt = list()

    def get_cursor(self):
        """Get a cursor to the current database connection.

:returns: A MonetDBLite cursor.
"""
        return self._connection.cursor()

    def close_connection(self):
        """Close the connection to the database"""

        self._connection.close()
        self._connection = None

    def is_connected(self):
        """Inquire if there is a live connection to the database.

:returns: `True` if there is an open database connection.
"""
        return self._connection is not None

    def get_limits(self):
        """Get the maximum IDs currently in the database for some tables.

While parsing traces we need to assign identifiers to various
objects. These need to be consistent with what is there in the
database currently. These limits need to be supplied to
:class:`mal\_analytics.profiler\_parser.ProfilerObjectParser`. This
function returns a tuple containing these limits.

:returns: A tupple with the following elements:

          #. execution ID
          #. even ID
          #. variable ID
          #. heartbeat ID

"""

        execution_id_query = "SELECT MAX(execution_id) FROM mal_execution"
        event_id_query = "SELECT MAX(event_id) FROM profiler_event"
        variable_id_query = "SELECT MAX(variable_id) FROM mal_variable"
        heartbeat_id_query = "SELECT MAX(heartbeat_id) FROM heartbeat"

        results = self.execute_query(execution_id_query)
        execution_id = results[0][0] if results[0][0] is not None else 0

        results = self.execute_query(event_id_query)
        event_id = results[0][0] if results[0][0] is not None else 0

        results = self.execute_query(variable_id_query)
        variable_id = results[0][0] if results[0][0] is not None else 0

        results = self.execute_query(heartbeat_id_query)
        heartbeat_id = results[0][0] if results[0][0] is not None else 0

        return (execution_id, event_id, variable_id, heartbeat_id)

    def create_parser(self):
        """Create and initialize a new :class:`mal_analytics.profiler_parser.ProfilerObjectParser` object.

:returns: A new parser for MonetDB JSON Profiler objects
        """
        return ProfilerObjectParser(**self.get_limits())
