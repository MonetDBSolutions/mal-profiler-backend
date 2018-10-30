# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import shutil
import tempfile

import pytest

from mal_profiler import create_db


@pytest.fixture(scope='function')
def backend():
    """Initialize the backend"""
    db_path = tempfile.mkdtemp(suffix='_mdbl')
    print("Initializind directory: {}".format(db_path))
    connection = create_db.init_backend(db_path)

    yield (db_path, connection)
    connection.close()
    shutil.rmtree(db_path)
    print("Deleted directory: {}".format(db_path))
