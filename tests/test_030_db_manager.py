# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018-2019

import pytest

from mal_analytics import db_manager
from mal_analytics.exceptions import DatabaseManagerError


class TestDatabaseManager(object):
    def test_singleton_dbadapter(self, manager_object):
        dbpath = manager_object.get_dbpath()
        new_manager = db_manager.DatabaseManager(dbpath)
        assert new_manager is manager_object, "Objects are not the same"

    def test_execute_query(self, manager_object):
        result = manager_object.execute_query("SELECT count(*) AS pe_count FROM profiler_event")
        assert len(result['pe_count']) == 1
        assert result['pe_count'][0] == 0

    def test_execute_bad_query(self, manager_object):
        result = manager_object.execute_query("This is not a correct SQL query")
        assert result is None

    def test_limits_empty_db(self, manager_object):
        limits = manager_object.get_limits()
        for v in limits.values():
            assert v == 0

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

        truth = {
            'max_execution_id': 1,
            'max_event_id': 1456,
            'max_variable_id': 865,
            'max_heartbeat_id': 0,
            'max_prerequisite_id': 2474,
            'max_query_id': 1,
            'max_supervises_id': 1,
        }
        limits = manager_object.get_limits()
        for k, v in truth.items():
            assert limits[k] == v

    def test_insert_without_connection(self, manager_object):
        manager_object._disconnect()
        with pytest.raises(DatabaseManagerError):
            manager_object.insert_data("abc", None)
