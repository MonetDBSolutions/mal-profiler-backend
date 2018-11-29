# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018

import json
import logging
import mal_analytics.exceptions as exceptions


LOGGER = logging.getLogger(__name__)


class ProfilerObjectParser:
    '''A parser for the MonetDB profiler traces.

    The purpose of this class is to turn the JSON objects that the
    MonetDB profiler emmits into a representation ready to be inserted
    into a MonetDBLite-Python trace database.
    '''
    def __init__(self, connection):
        logging.basicConfig(level=logging.DEBUG)
        self._event_id = 0
        self._heartbeat_id = 0
        self._next_execution_id = 0
        self._execution_dict = dict()
        self._variable_id = 0
        self._states = {'start': 0, 'done': 1, 'pause': 2}
        self._connection = connection
        # All possible MAL variable types.
        # NOTE: This might need to be updated if new types are added.
        self._type_dict = {
            'bit': 1,
            'bte': 2,
            'sht': 3,
            'int': 4,
            'lng': 5,
            'hge': 6,
            'oid': 7,
            'flt': 8,
            'dbl': 9,
            'str': 10,
            'date': 11,
            'void': 12,
            'BAT': 13,
            'bat[:bit]': 14,
            'bat[:bte]': 15,
            'bat[:sht]': 16,
            'bat[:int]': 17,
            'bat[:lng]': 18,
            'bat[:hge]': 19,
            'bat[:oid]': 20,
            'bat[:flt]': 21,
            'bat[:dbl]': 22,
            'bat[:str]': 23,
            'bat[:date]': 24,
        }

    def _parse_variable(self, var_data):
        '''Parse a single MAL variable.

        :param var_data: A dictionary representing the JSON description of a MAL variable.
        :returns: a dictionary with the following fields

          ``type_id``
              The database identifier of the type of the variable (see ...).
          ``name``
              The name of the variable.
          ``alias``
              (???)
          ``is_persistent``
              If the variable is persistent ``True`` or intermediate ``False``.
          ``bid``
              BAT ID(???).
          ``count``
              If the variable refers to a BAT, how many elements are in the BAT.
          ``size``
              The size of the type (???).
          ``seqbase``
              (???)
          ``hghbase``
              (???)
          ``eol``
              If `True` then the variable can be garbage collected.
          ``mal_value``
              If the variable is scalar, this is its value.

        '''
        variable = {
            # "mal_execution_id": int(current_execution_id),
            "type_id": self._type_dict.get(var_data.get("type"), -1),
            "name": var_data.get('name'),
            "alias": var_data.get('alias'),
            "is_persistent": var_data.get('kind') == 'persistent',
            "bid": var_data.get('bid'),
            "count": var_data.get('count'),
            "size": var_data.get('size', 0),
            "seqbase": var_data.get('seqbase'),
            "hghbase": var_data.get('hghbase'),
            "eol": var_data.get('eol') == 1,
            "mal_value": var_data.get('value')
        }

        return variable

    def _parse_event(self, json_object):
        '''Parse a single profiler event

        :param json_object: A dictionary representing a JSON object emmited by the profiler.
        :returns: a tuple of 5 items:

            - A dictionary containing the event data
            - A list of prerequisite event ids
            - A list of referenced variables
            - A list of argument variable ids
            - A list of return variable ids
        '''

        event_data = {
            "session": json_object.get("session"),
            "tag": json_object.get("tag"),
            "pc": json_object.get("pc"),
            "execution_state": self._states.get(json_object.get("state")),
            "clk": json_object.get("clk"),
            "ctime": json_object.get("ctime"),
            "thread": json_object.get("thread"),
            "mal_function": json_object.get("function"),
            "usec": json_object.get("usec"),
            "rss": json_object.get("rss"),
            "size": json_object.get("size"),
            "long_statement": json_object.get("stmt"),
            "short_statement": json_object.get("short"),
            "instruction": json_object.get("instruction"),
            "mal_module": json_object.get("module")
        }

        prereq_list = json_object.get("prereq")
        referenced_vars = dict()
        variable_lists = dict()
        variable_lists["arg"] = list()
        variable_lists["ret"] = list()

        for var_kind in ["arg", "ret"]:
            for item in json_object.get(var_kind, []):
                parsed_var = self._parse_variable(item)
                var_name = parsed_var.get('name')
                if var_name is None:
                    raise exceptions.MalParserError('Unnamed variable')
                if var_name in referenced_vars:
                    raise exceptions.MalParserError('Variable named {} already in referenced_vars'.format(var_name))
                referenced_vars[var_name] = parsed_var
                variable_lists[var_kind].append(parsed_var.get('name'))

        return (event_data, prereq_list, referenced_vars, variable_lists["arg"], variable_lists["ret"])

    def _get_execution_id(self, session, tag):
        '''Return the execution id for the given session and tag

        Before inserting events in the database execute

        .. code-block:: sql

           SELECT max(execution_id) FROM mal_execution

        and add the result to all the execution ids.
        '''
        key = "{}:{}".format(session, tag)
        execution_id = self._execution_dict.get(key)
        if execution_id is None:
            self._next_execution_id += 1
            execution_id = self._next_execution_id
            self._execution_dict[key] = execution_id

        return execution_id

    def _parse_trace_stream(self, json_stream):
        '''Parce a list of json trace objects

        This will create a representation ready to be inserted into the
        database.
        '''
        executions = dict()
        for json_event in json_stream:
            event_data, prereq_list, referenced_vars, args, lists = self._parse_event(json_event)
            execution = self._get_execution_id(event_data.get('session'), event_data.get('tag'))

    def _parse_trace(self, json_object):
        pass
        # '''Parses a trace object and adds it in the database

        # '''
        # cursor = self._connection.cursor()
        # cnt = cursor.execute("SELECT execution_id FROM mal_execution WHERE server_session=%s AND tag=%s",
        #                      [json_object['session'], json_object['tag']])

        # current_session = json_object['session']
        # current_tag = json_object['tag']
        # if cnt == 0:
        #     self._next_execution_id += 1
        #     LOGGER.debug("Creating execution %d for session %s, tag %d",
        #                  self._next_execution_id,
        #                  json_object['session'],
        #                  json_object['tag'])
        #     self._execution_dict["{}:{}".format(current_session, current_tag)] = self._next_execution_id
        #     # keep
        #     cursor.execute("INSERT INTO mal_execution(server_session, tag) VALUES (%s, %s)",
        #                    [json_object['session'], json_object['tag']])
        # elif cnt > 1:
        #     msg = 'more than one executions for session {} and tag {}'.format(json_object['session'], json_object['tag'])
        #     LOGGER.error(msg)
        #     raise exceptions.IntegrityConstraintViolation(msg)

        # current_execution_id = self._execution_dict.get("{}:{}".format(json_object['session'], json_object['tag']))
        # self._event_id += 1
        # LOGGER.debug("parsing trace. event id: %d (session %s, tag: %d, pc: %d, state: %s)",
        #              self._event_id,
        #              json_object['session'],
        #              json_object['tag'],
        #              json_object['pc'],
        #              json_object['state'])

        # exec_state = self._states.get(json_object.get('state'))
        # # return
        # event_data = {
        #     "execution_id": current_execution_id,
        #     "pc": json_object.get('pc'),
        #     "execution_state": exec_state,
        #     "clk": json_object.get('clk'),
        #     "ctime": json_object.get('ctime'),
        #     "thread": json_object.get('thread'),
        #     "mal_function": json_object.get('function'),
        #     "usec": json_object.get('usec'),
        #     "rss": json_object.get('rss'),
        #     "size": json_object.get('size'),
        #     "long_statement": json_object.get('stmt'),
        #     "short_statement": json_object.get('short'),
        #     "instruction": json_object.get('instruction'),
        #     "mal_module": json_object.get('module')
        # }

        # # kick out
        # ins_event_qtext = """INSERT INTO profiler_event (mal_execution_id, pc,
        #                                                  execution_state, clk,
        #                                                  ctime, thread,
        #                                                  mal_function, usec, rss,
        #                                                  type_size, long_statement,
        #                                                  short_statement, instruction,
        #                                                  mal_module)
        #                      VALUES(%(execution_id)s, %(pc)s, %(execution_state)s,
        #                             %(clk)s, %(ctime)s, %(thread)s, %(mal_function)s,
        #                             %(usec)s, %(rss)s, %(size)s, %(long_statement)s,
        #                             %(short_statement)s, %(instruction)s,
        #                             %(mal_module)s)"""
        # cursor.execute(ins_event_qtext, event_data)

        # # Process prerequisite events.
        # for prereq in json_object.get('prereq'):
        #     cnt = cursor.execute("SELECT event_id FROM profiler_event WHERE mal_execution_id=%s AND pc=%s AND execution_state=1",
        #                          [current_execution_id, prereq])
        #     if cnt == 0:
        #         msg = "event with mal_execution_id {} and pc {} not found".format(current_execution_id, prereq)
        #         LOGGER.error(msg)
        #         raise exceptions.MissingDataError(msg)
        #     elif cnt > 1:
        #         msg = "mal_execution_id {} and pc {} for done state not unique".format(current_execution_id, prereq)
        #         LOGGER.error(msg)
        #         raise exceptions.IntegrityConstraintViolation(msg)
        #     prereq_event_id = int(cursor.fetchone()[0])
        #     # kick out
        #     ins_prereqs_qtext = """INSERT INTO prerequisite_events(prerequisite_event, consequent_event)
        #                            VALUES (%s, %s)"""
        #     cursor.execute(ins_prereqs_qtext, [prereq_event_id, self._event_id])

        # # The algorithm to process a variable list is exactly the
        # # same for returns and for arguments, so we should not be
        # # duplicating code.
        # var_list_tables = {
        #     'ret': 'return_variable_list',
        #     'arg': 'argument_variable_list'
        # }
        # for var_list_field in ('ret', 'arg'):
        #     var_list = json_object.get(var_list_field, list())
        #     for var in var_list:
        #         # Have we encountered this variable before?
        #         sel_var_qtext = "SELECT variable_id FROM mal_variable WHERE name=%s"
        #         cnt = cursor.execute(sel_var_qtext, var['name'])
        #         if cnt == 0:
        #             # Nope, first time we see this
        #             # variable. Insert it into the variables
        #             # table.
        #             self._variable_id += 1
        #             sel_type_qtext = "SELECT type_id FROM mal_type WHERE tname=%s"
        #             cnt = cursor.execute(sel_type_qtext, var['type'])
        #             if cnt == 0:
        #                 LOGGER.warning('Unkown type: {}'.format(var['type']))
        #                 LOGGER.warning('Ignoring variable: {}'.format(var['name']))
        #                 continue
        #             res = cursor.fetchone()
        #             type_id = res[0]
        #             variable_data = {
        #                 "name": var.get('name'),
        #                 "mal_execution_id": int(current_execution_id),
        #                 "alias": var.get('alias'),
        #                 "type_id": int(type_id),
        #                 "is_persistent": var.get('kind') == 'persistent',
        #                 "bid": var.get('bid'),
        #                 "count": var.get('count'),
        #                 "size": var.get('size'),
        #                 "seqbase": var.get('seqbase'),
        #                 "hghbase": var.get('hghbase'),
        #                 "eol": var.get('eol') == 0,
        #                 "mal_value": var.get('value')
        #             }
        #             # kick out
        #             mvar_qtext = """INSERT INTO mal_variable (name, mal_execution_id,
        #                                                       alias, type_id, is_persistent,
        #                                                       bid, var_count, var_size, seqbase,
        #                                                       hghbase, eol, mal_value)
        #                             VALUES (%(name)s, %(mal_execution_id)s, %(alias)s,
        #                                     %(type_id)s, %(is_persistent)s, %(bid)s,
        #                                     %(count)s, %(size)s, %(seqbase)s, %(hghbase)s, %(eol)s,
        #                                     %(mal_value)s)"""
        #             cursor.execute(mvar_qtext, variable_data)
        #             current_var_id = self._variable_id
        #         else:
        #             # Yup, make a note of the variable id.
        #             res = cursor.fetchone()
        #             current_var_id = res[0]

        #         var_list_data = {
        #             'variable_list_index': var.get('index'),
        #             'event_id': self._event_id,
        #             'variable_id': int(current_var_id)
        #         }

        #         # kick out
        #         lt_ins_qtext = """INSERT INTO {}(variable_list_index,
        #                                          event_id, variable_id)
        #                           VALUES (%(variable_list_index)s, %(event_id)s,
        #                           %(variable_id)s)""".format(var_list_tables[var_list_field])

        #         cursor.execute(lt_ins_qtext, var_list_data)
        # # Closing the cursor is probably not needed since it will get GC'd
        # # cursor.close()

    def _parse_heartbeat(self, json_object):
        '''Parses a heartbeat object and adds it to the database.

        '''
        pass
        # cursor = self._connection.cursor()
        # self._heartbeat_id += 1
        # LOGGER.debug("parsing heartbeat. event id:", self._heartbeat_id)
        # data_keys = ('server_session',
        #              'clk',
        #              'ctime',
        #              'rss',
        #              'nvcsw')
        # data = {(k, json_object.get(k)) for k in data_keys}
        # heartbeat_ins_qtext = """INSERT INTO heartbeat(server_session, clk,
        #                                                ctime, rss, nvcsw)
        #                          VALUES(%(server_session)s, %(clk)s, %(ctime)s,
        #                                 %(rss)s, %(nvcsw)s)"""
        # cursor.execute(heartbeat_ins_qtext, data)
        # cpl_ins_qtext = """INSERT INTO cpuload(heartbeat_id, val)
        #                     VALUES(%(heartbeat_id)s, %(val)s)"""
        # for c in json_object['cpuload']:
        #     cursor.execute(cpl_ins_qtext, {'heartbeat_id': self._heartbeat_id,
        #                                    'val': c})

    def parse_object(self, json_string):
        try:
            json_object = json.loads(json_string)
        except json.JSONDecodeError as json_error:
            LOGGER.warning("W001: Cannot parse object")
            LOGGER.warning(json_string)
            LOGGER.warning("Decoder reports %s", json_error)
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