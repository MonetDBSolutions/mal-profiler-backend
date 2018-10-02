# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import json
import logging
import mal_profiler.exceptions


LOGGER = logging.getLogger(__name__)


class ProfilerObjectParser:
    def __init__(self, connection):
        self._event_id = 0
        self._heartbeat_id = 0
        self._execution_id = 0
        self._variable_id = 0
        self._states = {'start': 0, 'done': 1, 'pause': 2}
        self._connection = connection

    def _parse_trace(self, json_object):
        '''Parses a trace object and adds it in the database

        '''
        cursor = self._connection.cursor()
        cnt = cursor.execute("SELECT execution_id FROM mal_execution WHERE server_session=%s AND tag=%s",
                             [json_object['session'], json_object['tag']])
        if cnt == 0:
            self._execution_id += 1
            cursor.execute("INSERT INTO mal_execution(server_session, tag) VALUES (%s, %s)",
                           [json_object['session'], json_object['tag']])
        elif cnt > 1:
            msg = 'more than one executions for session {} and tag {}'.format(json_object['session'], json_object['tag'])
            LOGGER.error(msg)
            raise exceptions.IntegrityConstraintViolation(msg)

        self._event_id += 1
        print("parsing trace. event id:", self._event_id)

        exec_state = self._states.get(json_object.get('state'))
        event_data = {
            "execution_id": self._execution_id,
            "pc": json_object.get('pc'),
            "execution_state": exec_state,
            "clk": json_object.get('clk'),
            "ctime": json_object.get('ctime'),
            "thread": json_object.get('thread'),
            "mal_function": json_object.get('function'),
            "usec": json_object.get('usec'),
            "rss": json_object.get('rss'),
            "size": json_object.get('size'),
            "long_statement": json_object.get('stmt'),
            "short_statement": json_object.get('short')
        }

        ins_event_qtext = """INSERT INTO profiler_event (mal_execution_id, pc,
                                                         execution_state, clk,
                                                         ctime, thread,
                                                         mal_function, usec, rss,
                                                         type_size, long_statement,
                                                         short_statement)
                             VALUES(%(execution_id)s, %(pc)s, %(execution_state)s,
                                    %(clk)s, %(ctime)s, %(thread)s, %(mal_function)s,
                                    %(usec)s, %(rss)s, %(size)s, %(long_statement)s,
                                    %(short_statement)s)"""
        cursor.execute(ins_event_qtext, event_data)

        # Process prerequisite events.
        for prereq in json_object.get('prereq'):
            cnt = cursor.execute("SELECT event_id FROM profiler_event WHERE mal_execution_id=%s AND pc=%s AND execution_state=1",
                                 [self._execution_id, prereq])
            if cnt == 0:
                msg = "event with mal_execution_id {} and pc {} not found".format(self._execution_id, prereq)
                LOGGER.error(msg)
                raise exceptions.MissingDataError(msg)
            elif cnt > 1:
                msg = "mal_execution_id {} and pc {} for done state not unique".format(self._execution_id, prereq)
                LOGGER.error(msg)
                raise exceptions.IntegrityConstraintViolation(msg)
            prereq_event_id = int(cursor.fetchone()[0])
            ins_prereqs_qtext = """INSERT INTO prerequisite_events(prerequisite_event, consequent_event)
                                   VALUES (%s, %s)"""
            cursor.execute(ins_prereqs_qtext, [prereq_event_id, self._event_id])

        # The algorithm to process a variable list is exactly the
        # same for returns and for arguments, so we should not be
        # duplicating code.
        var_list_tables = {
            'ret': 'return_variable_list',
            'arg': 'argument_variable_list'
        }
        for var_list_field in ('ret', 'arg'):
            var_list = json_object.get(var_list_field, list())
            for var in var_list:
                # Have we encountered this variable before?
                sel_var_qtext = "SELECT variable_id FROM mal_variable WHERE name=%s"
                cnt = cursor.execute(sel_var_qtext, var['name'])
                if cnt == 0:
                    # Nope, first time we see this
                    # variable. Insert it into the variables
                    # table.
                    self._variable_id += 1
                    sel_type_qtext = "SELECT type_id FROM mal_type WHERE tname=%s"
                    cnt = cursor.execute(sel_type_qtext, var['type'])
                    if cnt == 0:
                        LOGGER.warning('Unkown type: {}'.format(var['type']))
                        LOGGER.warning('Ignoring variable: {}'.format(var['name']))
                        continue
                    res = cursor.fetchone()
                    type_id = res[0]
                    variable_data = {
                        "name": var.get('name'),
                        "mal_execution_id": int(self._execution_id),
                        "alias": var.get('alias'),
                        "type_id": int(type_id),
                        "is_persistent": var.get('kind') == 'persistent',
                        "bid": var.get('bid'),
                        "count": var.get('count'),
                        "size": var.get('size'),
                        "seqbase": var.get('seqbase'),
                        "hghbase": var.get('hghbase'),
                        "eol": var.get('eol') == 0
                    }
                    mvar_qtext = """INSERT INTO mal_variable (name, mal_execution_id,
                                                              alias, type_id, is_persistent,
                                                              bid, var_count, var_size, seqbase,
                                                              hghbase, eol)
                                    VALUES (%(name)s, %(mal_execution_id)s, %(alias)s,
                                            %(type_id)s, %(is_persistent)s, %(bid)s,
                                            %(count)s, %(size)s, %(seqbase)s, %(hghbase)s, %(eol)s)"""
                    cursor.execute(mvar_qtext, variable_data)
                    current_var_id = self._variable_id
                else:
                    # Yup, make a note of the variable id.
                    res = cursor.fetchone()
                    current_var_id = res[0]

                var_list_data = {
                    'variable_list_index': var.get('index'),
                    'event_id': self._event_id,
                    'variable_id': int(current_var_id)
                }

                lt_ins_qtext = """INSERT INTO {}(variable_list_index,
                                                 event_id, variable_id)
                                  VALUES (%(variable_list_index)s, %(event_id)s,
                                  %(variable_id)s)""".format(var_list_tables[var_list_field])

                cursor.execute(lt_ins_qtext, var_list_data)
        # Closing the cursor is probably not needed since it will get GC'd
        # cursor.close()

    def _parse_heartbeat(self, json_object):
        '''Parses a heartbeat object and adds it to the database.

        '''
        cursor = self._connection.cursor()
        self._heartbeat_id += 1
        data_keys = ('server_session',
                     'clk',
                     'ctime',
                     'rss',
                     'nvcsw')
        data = {(k, json_object.get(k)) for k in data_keys}
        heartbeat_ins_qtext = """INSERT INTO heartbeat(server_session, clk,
                                                       ctime, rss, nvcsw)
                                 VALUES(%(server_session)s, %(clk)s, %(ctime)s,
                                        %(rss)s, %(nvcsw)s)"""
        cursor.execute(heartbeat_ins_qtext, data)
        cpl_ins_qtext = """INSERT INTO cpuload(heartbeat_id, val)
                            VALUES(%(heartbeat_id)s, %(val)s)"""
        for c in json_object['cpuload']:
            cursor.execute(cpl_ins_qtext, {'heartbeat_id': self._heartbeat_id,
                                           'val': c})

    def parse_object(self, json_string):
        try:
            json_object = json.loads(json_string)
        except json.JSONDecodeError as json_error:
            LOGGER.warning("W001: Cannot parse object")
            LOGGER.warning(json_string)
            LOGGER.warning("Decoder reports %s", json_string)
            return

        dispatcher = {
            'trace': self._parse_trace,
            'heartbeat': self._parse_heartbeat
        }

        source = json_object.get('source')
        if source is None:
            LOGGER.error("Unkown JSON object")
            LOGGER.error(">%s<", json_object['source'])
            return

        try:
            parse_func = dispatcher[source]
        except KeyError:
            # TODO raise exception?
            LOGGER.error("Unkown JSON object kind: %s", source)
            return

        try:
            parse_func(json_object)
        except exceptions.MalParserError as e:
            LOGGER.warning("Parsing JSON Object\n  %s\nfailed:\n  %s", json_object, e.msg)
            return
