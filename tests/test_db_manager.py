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

    # @pytest.mark.skip()
    def test_limits_full_db(self, manager_object, query_trace1):
        parser = manager_object.create_parser()
        parser.parse_trace_stream(query_trace1)

        parsed_data = parser.get_data()
        parser.clear_internal_state()

        # Note: For Python < 3.7 order of elements.values() is not
        # guaranteed. More specifically the the order of elements
        # in a dictionary is an implementation detail, so
        # different implementations can conceivably have different
        # element orders.
        #
        # For Python >= 3.7 the order of elements is guaranteed to
        # be the insertion order.
        #
        # For Python 3.6 the implementation is the same as that of
        # Python 3.7.
        #
        # See this explanation: https://stackoverflow.com/a/15479974
        manager_object.drop_constraints()
        for k, v in parsed_data.items():
            manager_object.insert_data(k, v)
        manager_object.add_constraints()

        (exid, evid, varid, hbid, erid) = manager_object.get_limits()
        assert exid == 1
        assert evid == 1456
        assert varid == 865
        assert hbid == 0
        assert erid == 2474
