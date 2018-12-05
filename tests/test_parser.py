# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018

import json

import pytest
from mal_analytics import exceptions
from mal_analytics import profiler_parser

class TestParser(object):
    def test_parse_single_variable(self, parser_object):
        '''Test parsing of a single variable'''
        json_input_str = """{"index":0,"name":"C_1502","alias":"sys.lineitem.l_shipdate","type":"bat[:oid]","bid":0,"count":0,"size":0,"eol":0}"""
        json_input = json.loads(json_input_str)

        execution_id = 42
        variable = parser_object._parse_variable(json_input, execution_id)
        variable_truth = {
            "variable_id": 1,
            "name": "C_1502",
            "mal_execution_id": execution_id,
            "alias": "sys.lineitem.l_shipdate",
            "is_persistent": False,
            "var_size": 0,
            "seqbase": None,
            "hghbase": None,
            "mal_value": None,
            "type_id": 20,
            "eol": False,
            "bid": 0,
            "var_count": 0,
            "parent": None,
            "list_index": 0
        }

        assert len(variable) == len(variable_truth)
        for k, v in variable.items():
            assert variable_truth.get(k) == v, "Assertion failed for field '{}'".format(k)

    def test_parse_single_event(self, parser_object):
        """Test parsing a single event"""
        json_input_str = """{"version":"11.32.0 (hg id: 903396fca5 (git))","source":"trace","clk":27720582,"ctime":1543501117800118,"thread":44,"function":"user.s0_1","pc":2027,"tag":9,"module":"algebra","instruction":"thetaselect","session":"cd31712f-032b-486e-86c4-f6f445d1394d","state":"start","usec":0,"rss":101,"size":0,"stmt":"C_2622=nil:bat[:oid] := algebra.thetaselect(X_2373=<tmp_544>[100020]:bat[:date], C_394=<tmp_272>[100020]:bat[:oid], \\\"1992-12-11\\\":date, \\\"<=\\\":str);","short":"C_2622[0]:= thetaselect( X_2373[100020], C_394[100020], 1992-12-11, \\\"<=\\\" )","prereq":[2025,2026],"ret":[{"index":0,"name":"C_2622","alias":"sys.lineitem.l_shipdate","type":"bat[:oid]","bid":0,"count":0,"size":0,"eol":0}],"arg":[{"index":1,"name":"X_2373","alias":"sys.lineitem.l_shipdate","type":"bat[:date]","view":"true","parent":798,"seqbase":5601120,"hghbase":5701140,"kind":"persistent","bid":356,"count":100020,"size":400080,"eol":1},{"index":2,"name":"C_394","type":"bat[:oid]","kind":"transient","bid":186,"count":100020,"size":0,"eol":1},{"index":3,"name":"X_262","type":"date","value":"1992-12-11","eol":0},{"index":4,"name":"X_69","type":"str","value":"\\\"<=\\\"","eol":0}]}"""
        json_input = json.loads(json_input_str)

        event_data, prereq_list, variables, event_variables = parser_object._parse_event(json_input)
        event_data_truth = {
            "event_id": 1,
            "mal_execution_id": 1,
            "version": "11.32.0 (hg id: 903396fca5 (git))",
            "pc": 2027,
            "execution_state": 0,
            "clk": 27720582,
            "ctime": 1543501117800118,
            "thread": 44,
            "mal_function": "user.s0_1",
            "usec": 0,
            "rss": 101,
            "size": 0,
            "long_statement": "C_2622=nil:bat[:oid] := algebra.thetaselect(X_2373=<tmp_544>[100020]:bat[:date], C_394=<tmp_272>[100020]:bat[:oid], \"1992-12-11\":date, \"<=\":str);",
            "short_statement": "C_2622[0]:= thetaselect( X_2373[100020], C_394[100020], 1992-12-11, \"<=\" )",
            "instruction": "thetaselect",
            "mal_module": "algebra"
        }
        prereq_list_truth = [2025, 2026]
        variables_truth = {
            "C_2622": {
                "variable_id": 1,
                "name": "C_2622",
                "alias": "sys.lineitem.l_shipdate",
                "type_id": 20,
                "bid": 0,
                "var_count": 0,
                "var_size": 0,
                "eol": 0
            },
            "X_2373": {
                "variable_id": 2,
                "name": "X_2373",
                "alias": "sys.lineitem.l_shipdate",
                "type_id": 24,
                "parent": 798,
                "seqbase": 5601120,
                "hghbase": 5701140,
                "is_persistent": True,
                "bid": 356,
                "var_count": 100020,
                "var_size": 400080,
                "eol": 1
            },
            "C_394": {
                "variable_id": 3,
                "name": "C_394",
                "type_id": 20,
                "is_persistent": False,
                "bid": 186,
                "var_count": 100020,
                "var_size": 0,
                "eol": 1
            },
            "X_262": {
                "variable_id": 4,
                "name": "X_262",
                "type_id": 11,
                "mal_value": "1992-12-11",
                "eol": 0
            },
            "X_69": {
                "variable_id": 5,
                "name": "X_69",
                "type_id": 10,
                "mal_value": "\"<=\"",
                "eol": 0
            }
        }
        event_variables_truth = [
            {
                "event_id": 1,
                "variable_list_index": 0,
                "variable_id": 1
            },
            {
                "event_id": 1,
                "variable_list_index": 1,
                "variable_id": 2
            },
            {
                "event_id": 1,
                "variable_list_index": 2,
                "variable_id": 3
            },
            {
                "event_id": 1,
                "variable_list_index": 3,
                "variable_id": 4
            },
            {
                "event_id": 1,
                "variable_list_index": 4,
                "variable_id": 5
            },
        ]
        arg_vars_truth = ["X_2373", "C_394", "X_262", "X_69"]
        ret_vars_truth = ["C_2622"]

        assert len(event_data) == len(event_data_truth)
        for k, v in event_data_truth.items():
            assert event_data.get(k) == v, "Failed for key '{}'".format(k)

        assert len(prereq_list_truth) == len(prereq_list)
        for v in prereq_list_truth:
            assert v in prereq_list, "Prerequisite event {} missing from list".format(v)

        assert len(event_variables_truth) == len(event_variables)
        for var, var_dict in variables_truth.items():
            assert var in variables, "Variable {} missing from variable list".format(var)
            for k, v in var_dict.items():
                assert variables.get(var).get(k) == v, "{}.get('{}') != {}".format(var, k, v)

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
