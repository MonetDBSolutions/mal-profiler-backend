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
        new_parser = db_manager.DatabaseManager(dbpath)
        assert new_parser is manager_object
