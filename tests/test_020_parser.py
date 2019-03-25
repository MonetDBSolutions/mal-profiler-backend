# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018-2019

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

        event_data, prereq_list, variables, event_variables, query_data, initiates_executions_data = parser_object._parse_event(json_input)
        event_data_truth = {
            "event_id": 1,
            "mal_execution_id": 1,
            "version": "11.32.0 (hg id: 903396fca5 (git))",
            "pc": 2027,
            "execution_state": 0,
            "relative_time": 27720582,
            "absolute_time": 1543501117800118,
            "thread": 44,
            "mal_function": "user.s0_1",
            "usec": 0,
            "rss": 101,
            "type_size": 0,
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
            },
            "C_394": {
                "variable_id": 3,
                "name": "C_394",
                "type_id": 20,
                "is_persistent": False,
                "bid": 186,
                "var_count": 100020,
                "var_size": 0,
            },
            "X_262": {
                "variable_id": 4,
                "name": "X_262",
                "type_id": 11,
                "mal_value": "1992-12-11",
            },
            "X_69": {
                "variable_id": 5,
                "name": "X_69",
                "type_id": 10,
                "mal_value": "\"<=\"",
            }
        }
        event_variables_truth = [
            {
                "event_id": 1,
                "variable_list_index": 0,
                "variable_id": 1,
                "eol": 0,
            },
            {
                "event_id": 1,
                "variable_list_index": 1,
                "variable_id": 2,
                "eol": 1,
            },
            {
                "event_id": 1,
                "variable_list_index": 2,
                "variable_id": 3,
                "eol": 1,
            },
            {
                "event_id": 1,
                "variable_list_index": 3,
                "variable_id": 4,
                "eol": 0,
            },
            {
                "event_id": 1,
                "variable_list_index": 4,
                "variable_id": 5,
                "eol": 0,
            },
        ]
        arg_vars_truth = ["X_2373", "C_394", "X_262", "X_69"]
        ret_vars_truth = ["C_2622"]

        assert len(event_data) == len(event_data_truth)
        for k, v in event_data_truth.items():
            assert event_data.get(k) == v, "event_data check failed for key '{}'".format(k)

        assert len(prereq_list_truth) == len(prereq_list)
        for v in prereq_list_truth:
            assert v in prereq_list, "Prerequisite event {} missing from list".format(v)

        assert len(event_variables_truth) == len(event_variables)
        for var, var_dict in variables_truth.items():
            assert var in variables, "Variable {} missing from variable list".format(var)
            for k, v in var_dict.items():
                assert variables.get(var).get(k) == v, "{}.get('{}') != {}".format(var, k, v)

    def test_parse_unnamed_variable_raises(self, parser_object):
        json_input_str = """{"source":"trace","clk":1073703375,"ctime":1532603484932132,"thread":15,"function":"user.main","pc":46,"tag":214,"session":"dc2c13b3-8bde-4706-8ee5-60703a176325","state":"start","usec":0,"rss":1969,"size":0,"nvcsw":1,"stmt":"C_1502=nil:bat[:oid] := algebra.thetaselect(X_1338=<tmp_3010>[18751184]:bat[:date], C_283=<tmp_2056>[18751184]:bat[:oid], \\\"1998-08-22\\\":date, \\\"<=\\\":str);","short":"C_1502[0]:= thetaselect( X_1338[18751184], C_283[18751184], 1998-08-22, \\\"<=\\\" )","prereq":[44,45],"ret":[{"index":"0","alias":"sys.lineitem.l_shipdate","type":"bat[:oid]","bid":0,"count":0,"size":0,"eol":0}],"arg":[{"index":"1","name":"X_1338","alias":"sys.lineitem.l_shipdate","type":"bat[:date]","view":"true","parent":"787","seqbase":"18751184","hghbase":"37502368","kind":"persistent","bid":"1544","count":"18751184","size":75004736,"eol":1},{"index":"2","name":"C_283","type":"bat[:oid]","kind":"transient","bid":"1070","count":"18751184","size":0,"eol":1},{"index":"3","name":"X_265","type":"date","value":"1998-08-22","eol":0},{"index":"4","name":"X_72","type":"str","value":"\\\"<=\\\"","eol":0}]}"""

        json_input = json.loads(json_input_str)
        with pytest.raises(exceptions.MalParserError):
            event_data, prereq_list, variables, arg_vars, ret_vars = parser_object._parse_event(json_input)

    def test_execution_id(self, parser_object):
        tag1 = 1
        tag2 = 2
        session = "dc2c13b3-8bde-4706-8ee5-60703a176325"

        id1 = parser_object._get_execution_id(session, tag1)
        assert id1 is None
        id1 = parser_object._create_new_execution(session, tag1, 'main', '12')
        id2 = parser_object._get_execution_id(session, tag1)
        id3 = parser_object._create_new_execution(session, tag2, 'main', '12')

        assert id1 == id2
        assert id2 != id3
        assert id1 >= 1
        assert id2 >= 1

    def test_empty_get_data(self, parser_object):
        result = parser_object.get_data()

        data_truth = {
            "mal_execution": [
                "execution_id",
                "server_session",
                "tag",
                "server_version",
                "user_function",
            ],
            "profiler_event": [
                "event_id",
                "mal_execution_id",
                "pc",
                "execution_state",
                "relative_time",
                "absolute_time",
                "thread",
                "mal_function",
                "usec",
                "rss",
                "type_size",
                "long_statement",
                "short_statement",
                "instruction",
                "mal_module"
            ],
            "prerequisite_events": [
                "prerequisite_relation_id",
                "prerequisite_event",
                "consequent_event"
            ],
            "mal_variable": [
                "variable_id",
                "name",
                "mal_execution_id",
                "alias",
                "type_id",
                "is_persistent",
                "bid",
                "var_count",
                "var_size",
                "seqbase",
                "hghbase",
                "mal_value",
                "parent",
            ],
            "event_variable_list": [
                "event_id",
                "variable_list_index",
                "variable_id",
                "eol",
            ],
            "query": [
                "query_id",
                "query_text",
                "query_label",
                "root_execution_id"
            ],
            "initiates_executions": [
                "initiates_executions_id",
                "parent_id",
                "child_id",
                "remote"
            ],
            "heartbeat": [
                "heartbeat_id",
                "server_session",
                "clk",
                "ctime",
                "rss",
                "nvcsw",
            ],
            "cpuload": [
                "cpuload_id",
                "heartbeat_id",
                "val",
            ],
        }

        assert len(data_truth) == len(result)
        for k, v in data_truth.items():
            assert k in result.keys()
            assert len(data_truth[k]) == len(result[k])
            for vk in v:
                assert vk in result[k]
                assert len(result[k][vk]) == 0

    def test_parse_single_trace(self, parser_object, query_trace1):
        truth = {
            "mal_execution": 1,
            "profiler_event": 1456,
            "prerequisite_events": 2474,
            "mal_variable": 865,
            "event_variable_list": 5636,
            "query": 1,
            "initiates_executions": 1,
            "heartbeat": 0,
            "cpuload": 0
        }
        parser_object.parse_trace_stream(query_trace1)

        result = parser_object.get_data()
        assert len(result) == len(truth)

        for table in result:
            for field in result[table]:
                assert len(result[table][field]) == truth[table], "Check failed for table '{}'".format(table)

    def test_parse_multiple_traces(self, parser_object, query_trace1, query_trace2):
        truth = {
            "mal_execution": 2,
            "profiler_event": 3074,
            "prerequisite_events": 6598,
            "mal_variable": 1886,
            "event_variable_list": 13418,
            "query": 2,
            "initiates_executions": 2,
            "heartbeat": 0,
            "cpuload": 0
        }

        parser_object.parse_trace_stream(query_trace1)
        parser_object.parse_trace_stream(query_trace2)

        result = parser_object.get_data()
        assert len(result) == len(truth)

        for table in result:
            for field in result[table]:
                assert len(result[table][field]) == truth[table], "Check failed for table '{}'".format(table)

    def test_parse_trace_multiple_executions(self, parser_object, supervisor_trace):
        truth = {
            "mal_execution": 3,
            "profiler_event": 116,
            "prerequisite_events": 122,
            "mal_variable": 88,
            "event_variable_list": 344,
            "query": 1,
            "initiates_executions": 3,
            "heartbeat": 0,
            "cpuload": 0
        }

        parser_object.parse_trace_stream(supervisor_trace)

        result = parser_object.get_data()
        assert len(result) == len(truth)

        for table in result:
            for field in result[table]:
                assert len(result[table][field]) == truth[table], "Check failed for table '{}'".format(table)

    def test_parse_distributed_traces(self, parser_object, supervisor_trace, worker1_trace, worker2_trace):
        truth = {
            "mal_execution": 33,
            "profiler_event": 280,
            "prerequisite_events": 142,
            "mal_variable": 182,
            "event_variable_list": 564,
            "query": 1,
            "initiates_executions": 7,
            "heartbeat": 0,
            "cpuload": 0
        }
        parser_object.parse_trace_stream(supervisor_trace)
        parser_object.parse_trace_stream(worker1_trace)
        parser_object.parse_trace_stream(worker2_trace)

        result = parser_object.get_data()
        assert len(result) == len(truth)

        for table in result:
            for field in result[table]:
                assert len(result[table][field]) == truth[table], "Check failed for table '{}'".format(table)

    def test_parse_distributed_traces_worker_first(self, parser_object, supervisor_trace, worker1_trace, worker2_trace):
        truth = {
            "mal_execution": 33,
            "profiler_event": 280,
            "prerequisite_events": 142,
            "mal_variable": 182,
            "event_variable_list": 564,
            "query": 1,
            "initiates_executions": 7,
            "heartbeat": 0,
            "cpuload": 0
        }
        parser_object.parse_trace_stream(worker1_trace)
        parser_object.parse_trace_stream(supervisor_trace)
        parser_object.parse_trace_stream(worker2_trace)

        result = parser_object.get_data()
        assert len(result) == len(truth)

        for table in result:
            for field in result[table]:
                assert len(result[table][field]) == truth[table], "Check failed for table '{}'".format(table)

    def test_clear_data(self, parser_object, query_trace1):
        parser_object.parse_trace_stream(query_trace1)
        parser_object.clear_internal_state()
        result = parser_object.get_data()

        for table in result:
            for field in result[table]:
                assert len(result[table][field]) == 0

    def test_heartbeat_parsing(self, parser_object):
        heartbeat_input_string = '{"source":"heartbeat","session":"edd5fb0e-9ae7-4943-bca1-68a1722a052f","clk": 46545189,"ctime":1552925625398220,"rss":81,"nvcsw":11,"state":"ping","cpuload":[0.47,0.47,0.42,0.44]}'
        json_input = json.loads(heartbeat_input_string)

        heartbeat_truth = {
            "heartbeat_id": 1,
            "server_session": "edd5fb0e-9ae7-4943-bca1-68a1722a052f",
            "clk": 46545189,
            "ctime": 1552925625398220,
            "rss": 81,
            "nvcsw": 11,
        }
        cpuinfo_truth = [
            {
                "cpuload_id": 1,
                "heartbeat_id": 1,
                "val": 0.47,
            },
            {
                "cpuload_id": 2,
                "heartbeat_id": 1,
                "val": 0.47,
            },
            {
                "cpuload_id": 3,
                "heartbeat_id": 1,
                "val": 0.42,
            },
            {
                "cpuload_id": 4,
                "heartbeat_id": 1,
                "val": 0.44,
            },
        ]

        hb, cpu = parser_object._parse_heartbeat(json_input)


        assert len(hb) == len(heartbeat_truth)
        for k, v in heartbeat_truth.items():
            assert hb.get(k) == v, "heartbeat check failed for key {}".format(k)

        assert len(cpuinfo_truth) == len(cpu)
        for c in zip(cpuinfo_truth, cpu):
            assert len(c[0]) == len(c[1])
            for k, v in c[0].items():
                assert c[1].get(k) == v, "cpuinfo check failed for key {}".format(k)

    def test_parse_variable_persistence(self, parser_object, query_trace1):
        persistent_vars = 70

        parser_object.parse_trace_stream(query_trace1)

        result = parser_object.get_data()

        count = 0
        for persistent in result['mal_variable']['is_persistent']:
            if persistent:
                count += 1

        assert persistent_vars == count, "Wrong number of persistent variables"
