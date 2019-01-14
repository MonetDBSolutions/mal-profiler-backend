# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018-2019

from mal_analytics import trace_reader

class TestTraceReader(object):
    def test_gzip_tester(self, filenames):
        assert not trace_reader.is_gzip(filenames[0])
        assert trace_reader.is_gzip(filenames[1])
        assert not trace_reader.is_gzip(filenames[2])

    def test_bzip2_tester(self, filenames):
        assert not trace_reader.is_bzip2(filenames[0])
        assert not trace_reader.is_bzip2(filenames[1])
        assert trace_reader.is_bzip2(filenames[2])

    def test_abstract_open(self, filenames):
        for fln in filenames:
            fl = trace_reader.abstract_open(fln)
            assert fl.readable()
            fl.close()

    def test_read_objects(self, filenames):
        fl = trace_reader.abstract_open(filenames[0])

        cnt = 0
        json_string = trace_reader.read_object(fl)
        while json_string:
            cnt += 1
            json_string = trace_reader.read_object(fl)

        fl.close()
        assert cnt == 1456
