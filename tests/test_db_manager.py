# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018

import pytest

from mal_analytics import db_manager


class TestDatabaseManager(object):
    def test_singleton_dbadapter(self, manager_object):
        dbpath = manager_object.get_dbpath()
        new_manager = db_manager.DatabaseManager(dbpath)
        assert new_manager is manager_object, "Objects are not the same"

    def test_execute_query(self, manager_object):
        result = manager_object.execute_query("SELECT count(*) FROM profiler_event")
        assert result == [[0]]

    def test_execute_bad_query(self, manager_object):
        result = manager_object.execute_query("This is not a correct SQL query")
        assert result is None

    def test_limits_empty_db(self, manager_object):
        # result = manager_object.execute_query("SELECT * FROM mal_execution")
        # print(result)
        (exid, evid, varid, hbid, erid) = manager_object.get_limits()
        assert exid == 0
        assert evid == 0
        assert varid == 0
        assert hbid == 0
        assert erid == 0
