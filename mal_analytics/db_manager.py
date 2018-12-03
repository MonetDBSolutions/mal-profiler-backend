# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018

import logging
import os
import monetdblite

LOGGER = logging.getLogger(__name__)


class Singleton(type):
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

    See also `this <https://stackoverflow.com/a/6798042>`_
    stackoverflow post.

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
        """Get the directory of the database.

        :returns: The path where the database has been initialized.
        """
        return self._dbpath

    def execute_query(self, query):
        cursor = self._connection.cursor()
        try:
            cursor.execute(query)
            results = cursor.fetchall()
        except monetdblite.Error as e:
            LOGGER.warning("query\n  %s\n failed with message: %s", query, e)
            results = None

        return results

    def execute_sql_script(self, script_path):
        with open(script_path) as sql_fl:
            sql_in = sql_fl.read()

        for stmt in sql_in.split(';')[:-1]:
            self._connection.execute(stmt)

    def get_cursor(self):
        return self._connection.cursor()

    def close_connection(self):
        self._connection.close()
        self._connection = None

    def is_connected(self):
        return self._connection is not None

    def get_limits(self):
        execution_id_query = "SELECT MAX(execution_id) FROM mal_execution"
        event_id_query = "SELECT MAX(event_id) FROM profiler_event"
        variable_id_query = "SELECT MAX(variable_id) FROM mal_variable"
        heartbeat_id_query = "SELECT MAX(heart_id) FROM heartbeat"

        results = self.execute_query(execution_id_query)
        execution_id = results[0] if results is not None else 0

        results = self.execute_query(event_id_query)
        event_id = results[0] if results is not None else 0

        results = self.execute_query(variable_id_query)
        variable_id = results[0] if results is not None else 0

        results = self.execute_query(heartbeat_id_query)
        heartbeat_id = results[0] if results is not None else 0

        return (execution_id, event_id, variable_id, heartbeat_id)
