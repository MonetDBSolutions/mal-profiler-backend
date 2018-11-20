# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018


class MalParserError(Exception):
    """Base class for any exception"""
    pass


class IntegrityConstraintViolation(MalParserError):
    pass


class MissingDataError(MalParserError):
    pass
