# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018

import os
import monetdblite
import pytest

from mal_analytics import DatabaseManager


class TestCreation(object):
    def test_table_creation(self, manager_object):
        '''Make sure that all the tables get created correctly'''

        cursor = manager_object.get_cursor()
        tables = [
            'mal_execution',
            'profiler_event',
            'prerequisite_events',
            'mal_type',
            'mal_variable',
            'event_variable_list',
            'query',
            'query_executions',
            'supervises_executions',
            'heartbeat',
            'cpuload'
        ]
        for tbl in tables:
            rslt = cursor.execute("SELECT id FROM _tables WHERE name =%s", tbl)
            assert rslt == 1

        cursor.close()

    def test_restart(self, manager_object):
        db_directory = manager_object.get_dbpath()
        manager_object.close_connection()
        new_manager = DatabaseManager(db_directory)
        self.test_table_creation(new_manager)

    def test_sql_execution(self, manager_object):
        spath = os.path.dirname(os.path.abspath(__file__))
        manager_object.execute_sql_script(os.path.join(spath, 'data', 'test_sql_script.sql'))
        cursor = manager_object.get_cursor()

        rslt = cursor.execute("SELECT tag FROM mal_execution")
        assert rslt == 6
        rslt = cursor.execute("SELECT tag FROM mal_execution WHERE server_session='a11bd16e-dc2d-11e8-aa5e-e4b318554ad8'")
        assert rslt == 3

    def test_reinitialization(self, manager_object):
        db_directory = manager_object.get_dbpath()
        spath = os.path.dirname(os.path.abspath(__file__))
        manager_object.execute_sql_script(os.path.join(spath, 'data', 'test_sql_script.sql'))
        manager_object.close_connection()

        new_manager = DatabaseManager(db_directory)
        cursor = new_manager.get_cursor()
        rslt = cursor.execute("SELECT tag FROM mal_execution")
        assert rslt == 6
        rslt = cursor.execute("SELECT tag FROM mal_execution WHERE server_session='a11bd16e-dc2d-11e8-aa5e-e4b318554ad8'")
        assert rslt == 3

    def test_double_initialize_table_indempotence(self, manager_object):
        manager_object._initialize_tables()

        self.test_table_creation(manager_object)
