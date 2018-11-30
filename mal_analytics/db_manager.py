# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018

import logging

import monetdblite

LOGGER = logging.getLogger(__name__)


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class DatabaseManager(object, metaclass=Singleton):
    """A connection manager for the database.

    This class is a *signleton*. Singletons are to be avoided in
    general, but unfortunatelly any component communicating directly
    with MonetDBLite should be a singleton, because of the way
    MonetDBLite operates.

    See also `this <https://stackoverflow.com/a/6798042>`_
    stackoverflow post.

    """

    def __init__(self, dbpath):
        self._dbpath = dbpath
        self._connection = monetdblite.make_connection(dbpath)

    def get_dbpath(self):
        return self._dbpath
