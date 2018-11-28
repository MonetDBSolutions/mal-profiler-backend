# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018

import json

import pytest
from mal_profiler import exceptions

class TestParser(object):
    def test_parse_single_variable(self, parser_object):
        '''Test parsing of a single variable'''
        json_input_str = """{"index":"0","name":"C_1502","alias":"sys.lineitem.l_shipdate","type":"bat[:oid]","bid":"0","count":"0","size":0,"eol":0}"""
        json_input = json.loads(json_input_str)

        variable = parser_object._parse_variable(json_input)
        variable_truth = {
            "name": "C_1502",
            "alias": "sys.lineitem.l_shipdate",
            "is_persistent": False,
            "size": 0,
            "seqbase": None,
            "hghbase": None,
            "mal_value": None,
            "type_id": 20,
            "eol": False,
            # TODO: fix type in MonetDB server
            "bid": "0",
            "count": "0",
        }

        assert len(variable) == len(variable_truth)
        for k, v in variable_truth.items():
            assert variable.get(k) == v

    def test_parse_single_event(self, parser_object):
        """Test parsing a single event"""
        json_input_str = """{"source":"trace","clk":1073703375,"ctime":1532603484932132,"thread":15,"function":"user.main","pc":46,"tag":214,"session":"dc2c13b3-8bde-4706-8ee5-60703a176325","state":"start","usec":0,"rss":1969,"size":0,"nvcsw":1,"stmt":"C_1502=nil:bat[:oid] := algebra.thetaselect(X_1338=<tmp_3010>[18751184]:bat[:date], C_283=<tmp_2056>[18751184]:bat[:oid], \\\"1998-08-22\\\":date, \\\"<=\\\":str);","short":"C_1502[0]:= thetaselect( X_1338[18751184], C_283[18751184], 1998-08-22, \\\"<=\\\" )","prereq":[44,45],"ret":[{"index":"0","name":"C_1502","alias":"sys.lineitem.l_shipdate","type":"bat[:oid]","bid":"0","count":"0","size":0,"eol":0}],"arg":[{"index":"1","name":"X_1338","alias":"sys.lineitem.l_shipdate","type":"bat[:date]","view":"true","parent":"787","seqbase":"18751184","hghbase":"37502368","kind":"persistent","bid":"1544","count":"18751184","size":75004736,"eol":1},{"index":"2","name":"C_283","type":"bat[:oid]","kind":"transient","bid":"1070","count":"18751184","size":0,"eol":1},{"index":"3","name":"X_265","type":"date","value":"1998-08-22","eol":0},{"index":"4","name":"X_72","type":"str","value":"\\\"<=\\\"","eol":0}]}"""
        json_input = json.loads(json_input_str)

        event_data, prereq_list, variables, arg_vars, ret_vars = parser_object._parse_event(json_input)
        event_data_truth = {
            "session": "dc2c13b3-8bde-4706-8ee5-60703a176325",
            "tag": 214,
            "pc": 46,
            "execution_state": 0,
            "clk": 1073703375,
            "ctime": 1532603484932132,
            "thread": 15,
            "mal_function": "user.main",
            "usec": 0,
            "rss": 1969,
            "size": 0,
            "long_statement": "C_1502=nil:bat[:oid] := algebra.thetaselect(X_1338=<tmp_3010>[18751184]:bat[:date], C_283=<tmp_2056>[18751184]:bat[:oid], \"1998-08-22\":date, \"<=\":str);",
            "short_statement": "C_1502[0]:= thetaselect( X_1338[18751184], C_283[18751184], 1998-08-22, \"<=\" )",
            "instruction": None,
            "module": None
        }
        prereq_list_truth = [44, 45]
        variables_truth = {
            "X_1338": {
                "name": "X_1338",
                "alias": "sys.lineitem.l_shipdate",
                "is_persistent": True,
                "size": 75004736,
                "seqbase": "18751184",
                "hghbase": "37502368",
                "mal_value": None,
                "type_id": 24,
                "eol": True,
                # TODO: fix type in MonetDB server
                "bid": "1544",
                "count": "18751184",
            },
            "C_283": {
                "name": "C_283",
                "alias": None,
                "is_persistent": False,
                "size": 0,
                "seqbase": None,
                "hghbase": None,
                "mal_value": None,
                "type_id": 20,
                "eol": True,
                # TODO: fix type in MonetDB server
                "bid": "1070",
                "count": "18751184",
            },
            "X_265": {
                "name": "X_265",
                "alias": None,
                "is_persistent": False,
                "size": 0,
                "seqbase": None,
                "hghbase": None,
                "mal_value": "1998-08-22",
                "type_id": 11,
                "eol": False,
                # TODO: fix type in MonetDB server
                "bid": None,
                "count": None,
            },
            "X_72": {
                "name": "X_72",
                "alias": None,
                "is_persistent": False,
                "size": 0,
                "seqbase": None,
                "hghbase": None,
                "mal_value": '"<="',
                "type_id": 10,
                "eol": False,
                # TODO: fix type in MonetDB server
                "bid": None,
                "count": None,
            },
            "C_1502": {
                "name": "C_1502",
                "alias": "sys.lineitem.l_shipdate",
                "is_persistent": False,
                "size": 0,
                "seqbase": None,
                "hghbase": None,
                "mal_value": None,
                "type_id": 20,
                "eol": False,
                # TODO: fix type in MonetDB server
                "bid": "0",
                "count": "0",
            },
        }
        arg_vars_truth = ["X_1338", "C_283", "X_265", "X_72"]
        ret_vars_truth = ["C_1502"]

        assert len(event_data) == len(event_data_truth)
        for k, v in event_data_truth.items():
            assert event_data.get(k) == v

        assert len(prereq_list_truth) == len(prereq_list)
        for v in prereq_list_truth:
            assert v in prereq_list

        assert len(variables_truth) == len(variables)
        for var, var_dict in variables_truth.items():
            assert var in variables
            for k, v in var_dict.items():
                assert variables.get(var).get(k) == v

        assert len(arg_vars_truth) == len(arg_vars)
        for v in arg_vars_truth:
            assert v in arg_vars

        assert len(ret_vars_truth) == len(ret_vars)
        for v in ret_vars_truth:
            assert v in ret_vars

    def test_parse_unnamed_variable_raises(self, parser_object):
        json_input_str = """{"source":"trace","clk":1073703375,"ctime":1532603484932132,"thread":15,"function":"user.main","pc":46,"tag":214,"session":"dc2c13b3-8bde-4706-8ee5-60703a176325","state":"start","usec":0,"rss":1969,"size":0,"nvcsw":1,"stmt":"C_1502=nil:bat[:oid] := algebra.thetaselect(X_1338=<tmp_3010>[18751184]:bat[:date], C_283=<tmp_2056>[18751184]:bat[:oid], \\\"1998-08-22\\\":date, \\\"<=\\\":str);","short":"C_1502[0]:= thetaselect( X_1338[18751184], C_283[18751184], 1998-08-22, \\\"<=\\\" )","prereq":[44,45],"ret":[{"index":"0","alias":"sys.lineitem.l_shipdate","type":"bat[:oid]","bid":"0","count":"0","size":0,"eol":0}],"arg":[{"index":"1","name":"X_1338","alias":"sys.lineitem.l_shipdate","type":"bat[:date]","view":"true","parent":"787","seqbase":"18751184","hghbase":"37502368","kind":"persistent","bid":"1544","count":"18751184","size":75004736,"eol":1},{"index":"2","name":"C_283","type":"bat[:oid]","kind":"transient","bid":"1070","count":"18751184","size":0,"eol":1},{"index":"3","name":"X_265","type":"date","value":"1998-08-22","eol":0},{"index":"4","name":"X_72","type":"str","value":"\\\"<=\\\"","eol":0}]}"""

        json_input = json.loads(json_input_str)
        with pytest.raises(exceptions.MalParserError):
            event_data, prereq_list, variables, arg_vars, ret_vars = parser_object._parse_event(json_input)


    def test_execution_id(self, parser_object):
        tag1 = 1
        tag2 = 2
        session = "dc2c13b3-8bde-4706-8ee5-60703a176325"

        id1 = parser_object._get_execution_id(session, tag1)
        id2 = parser_object._get_execution_id(session, tag1)
        id3 = parser_object._get_execution_id(session, tag2)

        assert id1 == id2
        assert id2 != id3
        assert id1 >= 1
        assert id2 >= 1

    @pytest.mark.skip()
    def test_parse_single_trace(self, query_trace):
        assert False

    @pytest.mark.skip()
    def test_parse_multiple_traces(self, query_trace1, query_trace2):
        assert False
