# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018-2019


class AnalyticsException(Exception):
    pass

class MalParserError(AnalyticsException):
    """Base class for any parser exception"""
    pass


class IntegrityConstraintViolation(MalParserError):
    """Gets raised if a integrity constraint (e.g. primary/foreign keys) is violated.
    """
    pass


class MissingDataError(MalParserError):
    pass


class DatabaseManagerError(AnalyticsException):
    pass


class InitializationError(DatabaseManagerError):
    pass
