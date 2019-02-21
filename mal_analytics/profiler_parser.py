# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018-2019

import logging
import re

import mal_analytics.exceptions as exceptions


LOGGER = logging.getLogger(__name__)


class ProfilerObjectParser(object):
    '''A parser for the MonetDB profiler traces.

The purpose of this class is to turn the JSON objects that the
MonetDB profiler emmits into a representation ready to be inserted
into a MonetDBLite-Python trace database.

:param execution_id: The maximum execution id in the database
:param event_id: The maximum event id in the database
:param variable_id: The maximum variable id in the database
:param heartbeat_id: The maximum heartbeat id in the database
:param prereq_id: The maximum prerequisite relation id in the database
:param query_id: The maximum query id in the database
:param supervises_executions_id: The maximum query_executions id in the database
'''

    def __init__(self, limits=dict()):
        logging.basicConfig(level=logging.DEBUG)
        self._execution_id = limits.get('max_execution_id', 0)
        self._event_id = limits.get('max_event_id', 0)
        self._variable_id = limits.get('max_variable_id', 0)
        self._heartbeat_id = limits.get('max_heartbeat_id', 0)
        self._prerequisite_relation_id = limits.get('max_prerequisite_id', 0)
        self._query_id = limits.get('max_query_id', 0)
        self._initiates_executions_id = limits.get('max_initiates_id', 0)

        self._initiates_association = dict()
        self._var_name_to_id = dict()
        self._execution_dict = dict()
        self._states = {'start': 0, 'done': 1, 'pause': 2}

        self._initialize_tables()

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

    def _initialize_tables(self):
        """Initialize dictionaries that map directly to the db tables.
"""
        self._executions = {
            "execution_id": list(),
            "server_session": list(),
            "tag": list(),
            "server_version": list(),
            "user_function": list(),
        }

        self._events = {
            "event_id": list(),
            "mal_execution_id": list(),
            "pc": list(),
            "execution_state": list(),
            "clk": list(),
            "ctime": list(),
            "thread": list(),
            "mal_function": list(),
            "usec": list(),
            "rss": list(),
            "type_size": list(),
            "long_statement": list(),
            "short_statement": list(),
            "instruction": list(),
            "mal_module": list(),
        }

        self._prerequisite_events = {
            "prerequisite_relation_id": list(),
            "prerequisite_event": list(),
            "consequent_event": list(),
        }

        self._variables = {
            "variable_id": list(),
            "name": list(),
            "mal_execution_id": list(),
            "alias": list(),
            "type_id": list(),
            "is_persistent": list(),
            "bid": list(),
            "var_count": list(),
            "var_size": list(),
            "seqbase": list(),
            "hghbase": list(),
            "eol": list(),
            "mal_value": list(),
            "parent": list(),
        }

        self._event_variables = {
            "event_id": list(),
            "variable_list_index": list(),
            "variable_id": list(),
        }

        # BUG: If I remove query_text or root_execution_id
        # test_limits_full_db coredumps on manager.insert_data
        # (monetdblite.insert?).
        self._query = {
            "query_id": list(),
            "query_text": list(),
            "query_label": list(),
            "root_execution_id": list(),
        }

        self._initiates_executions = {
            "initiates_executions_id": list(),
            "parent_id": list(),
            "child_id": list(),
            "remote": list ()
        }

        self._heartbeats = {
            "heartbeat_id": list(),
            "server_session": list(),
            "clk": list(),
            "ctime": list(),
            "rss": list(),
            "nvcsw": list(),
        }

        self._cpuloads = {
            "cpuload_id": list(),
            "heartbeat_id": list(),
            "val": list(),
        }


    def _parse_variable(self, var_data, current_execution_id):
        '''Parse a single MAL variable.

:param var_data: A dictionary representing the JSON description of a MAL variable.
:returns: A dictionary representing a variable. See :ref:`data_structures`.
'''
        var_key = "{}:{}".format(current_execution_id, var_data.get("name"))
        var_id = self._var_name_to_id.get(var_key)

        new_var = False
        if var_id is None:
            self._variable_id += 1
            var_id = self._variable_id
            new_var = True

        # LOGGER.debug("variable id = %d, new var = %d", var_id, new_var)
        bid = var_data.get('bid', -1)
        # bid can have the value MIN_INT - 1 if it has been garbage
        # collected. Maybe?
        if bid < 0:
            bid = None
        variable = {
            "variable_id": var_id,
            "mal_execution_id": current_execution_id,
            "type_id": self._type_dict.get(var_data.get("type"), -1),
            "name": var_data.get('name'),
            "alias": var_data.get('alias'),
            "is_persistent": var_data.get('kind') == 'persistent',
            "bid": bid,
            "var_count": var_data.get('count'),
            "var_size": var_data.get('size', 0),
            "seqbase": var_data.get('seqbase'),
            "hghbase": var_data.get('hghbase'),
            "eol": var_data.get('eol') == 1,
            "mal_value": var_data.get('value'),
            "parent": var_data.get('parent'),
            "list_index": var_data.get('index')
        }
        if new_var:
            self._var_name_to_id[var_key] = self._variable_id

        return variable

    def _parse_query_text(self, short_description):
        p = re.compile(r"^define\((.+)\)")
        m = p.match(short_description)
        qtext = None
        if m:
            define_arguments = m.group(1)
            # The following assumes that the arguments to
            # querylog.define are 3 and the last two arguments do not
            # contain the character ','.
            end = define_arguments.rfind(',', 0, define_arguments.rfind(','))
            qtext = define_arguments[:end].strip()
            qtext = qtext[1:-1]  # remove quotes

        return qtext

    def _parse_event(self, json_object):
        '''Parse a single profiler event

:param json_object: A dictionary representing a JSON object emmited by the profiler.
:returns: a tuple of 5 items:

    - A dictionary containing the event data (see :ref:`data_structures`)
    - A list of prerequisite event ids
    - A list of referenced variables (see :ref:`data_structures`)
    - A list of argument variable ids
    - A list of return variable ids
'''

        self._event_id += 1
        current_execution_id = self._get_execution_id(json_object.get('session'), json_object.get('tag'))
        if current_execution_id is None:
            instruction = json_object.get('instruction')
            if json_object.get('pc') != 0:
                # Surprisingly this can happen. I noticed it on remote
                # table queries.
                LOGGER.warning("Instruction with pc==0 missing for execution {}:{}".format(
                    json_object.get('session'),
                    json_object.get('tag')
                ))
                instruction = json_object.get('function').split('.')[1]
            current_execution_id = self._set_execution_id(
                json_object.get('session'),
                json_object.get('tag'),
                instruction,
                json_object.get('version')
            )

        event_data = {
            "event_id": self._event_id,
            "mal_execution_id": current_execution_id,
            "pc": json_object.get("pc"),
            "execution_state": self._states.get(json_object.get("state")),
            "clk": json_object.get("clk"),
            "ctime": json_object.get("ctime"),
            "thread": json_object.get("thread"),
            "mal_function": json_object.get("function"),
            "usec": json_object.get("usec"),
            "rss": json_object.get("rss"),
            "type_size": json_object.get("size"),
            "long_statement": json_object.get("stmt"),
            "short_statement": json_object.get("short"),
            "instruction": json_object.get("instruction"),
            "mal_module": json_object.get("module"),
            "version": json_object.get("version"),
        }

        prereq_list = json_object.get("prereq")
        referenced_vars = dict()
        event_variables = list()

        for var_kind in ["ret", "arg"]:
            for item in json_object.get(var_kind, []):
                parsed_var = self._parse_variable(item, current_execution_id)
                var_name = parsed_var.get('name')
                if var_name is None:
                    raise exceptions.MalParserError('Unnamed variable')
                referenced_vars[var_name] = parsed_var
                event_variables.append({
                    "event_id": self._event_id,
                    "variable_list_index": parsed_var.get('list_index'),
                    "variable_id": parsed_var.get('variable_id')
                })

        query_data = None
        initiates_executions_data = list()
        if event_data['mal_module'] == 'querylog' and event_data['instruction'] == 'define' and event_data['execution_state'] == 0:
            query_data, new_initiates_relations = self._register_new_query(event_data, current_execution_id)
            initiates_executions_data.extend(new_initiates_relations)

        elif event_data['mal_module'] == 'remote' and event_data['instruction'] == 'register_supervisor' and event_data['execution_state'] == 0:
            self._handle_remote_initiates(json_object, referenced_vars, current_execution_id, initiates_executions_data)
        elif event_data['mal_module'] == 'user' and event_data['execution_state'] == 0:
            LOGGER.debug("\n  event data['instruction'] = %s\n  event_data['short_statement'] = %s\n  current execution = %d", event_data.get('instruction'), event_data.get('short_statement'), current_execution_id)
            self._handle_local_initiates(event_data, current_execution_id, initiates_executions_data)

        return (
            event_data,
            prereq_list,
            referenced_vars,
            event_variables,
            query_data,
            initiates_executions_data
        )

    def _set_execution_id(self, session, tag, user_function, server_version=None):
        key = "{}:{}".format(session, tag)
        execution_id = self._execution_dict.get(key)

        if execution_id is None:
            self._execution_id += 1
            execution_id = self._execution_id
            self._execution_dict[key] = execution_id

            # Add the new execution to the table.
            self._executions['execution_id'].append(execution_id)
            self._executions['server_session'].append(session)
            self._executions['tag'].append(tag)
            self._executions['user_function'].append(user_function)
            self._executions['server_version'].append(server_version)

            return execution_id
        else:
            raise exceptions.MalParserError("execution for session {}, tag {} already registered".format(session, tag))

    def _handle_local_initiates(self, event_data, current_execution_id, initiates_executions_data):
        idx = self._executions["execution_id"].index(current_execution_id)
        server_session = self._executions["server_session"][idx]

        # We are concatenating the server_session with the function
        # name. We are assuming that the function calls are local
        # within a server session. For the remote case we need to
        # detect the register_supervisor call.
        key = "{}:{}".format(server_session, event_data["instruction"])

        # We are defining a function...
        if event_data['short_statement'].startswith('function'):
            LOGGER.debug("Defining")
            lookup_key = key + ":c"  # ...so we look up the *call* of the function
            if lookup_key in self._initiates_association:
                LOGGER.debug("  Resolving key %s", key)
                self._initiates_executions_id += 1
                initiates_executions_data.append({
                    "initiates_executions_id": self._initiates_executions_id,
                    "parent_id": self._initiates_association[lookup_key],
                    "child_id": current_execution_id,
                    "remote": False,
                })
                del self._initiates_association[lookup_key]
            else:
                LOGGER.debug("  Recording key %s", key)
                record_key = key + ":d"
                self._initiates_association[record_key] = current_execution_id
        else:
            # We are calling a function...
            LOGGER.debug("Calling")
            lookup_key = key + ":d"  # ...so we look up the *definition* of the function
            if lookup_key in self._initiates_association:
                LOGGER.debug("  Resolving key %s", key)
                self._initiates_executions_id += 1
                initiates_executions_data.append ({
                    "initiates_executions_id": self._initiates_executions_id,
                    "parent_id": current_execution_id,
                    "child_id": self._initiates_association[lookup_key],
                    "remote": False,
                })
                del self._initiates_association[lookup_key]
            else:
                LOGGER.debug("  Recording key %s", key)
                record_key = key + ":c"
                self._initiates_association[record_key] = current_execution_id
                

    def _register_new_query(self, event_data, current_execution_id):
        initiates_executions_data = list()
        self._query_id += 1
        query_data = {
            "query_id": self._query_id,
            "query_text": self._parse_query_text(event_data['short_statement']),  # TODO: find the value of the variable with index=1
            "query_label": None,
            "root_execution_id": event_data['mal_execution_id']
        }
        LOGGER.debug('Adding query {}, id: {}'.format(query_data['query_text'], query_data['query_id']))
        # An execution with a call to querylog.define supervises
        # itself
        self._initiates_executions_id += 1
        initiates_executions_data.append({
            "initiates_executions_id": self._initiates_executions_id,
            "parent_id": current_execution_id,
            "child_id": current_execution_id,
            "remote": False,
        })

        return (query_data, initiates_executions_data)

    def _handle_remote_initiates(self, json_object, referenced_vars, current_execution_id, initiates_executions_data):
        # In queries over remote tables the plan is split in
        # several executions. One of these is the supervisor
        # execution that orchestrates the worker executions. The
        # server emmits a call to the remote.register_supervisor
        # MAL instruction in all of these plans. This instruction
        # is effectively a noop. The idea is to associate the
        # supervisor execution with the workers in a systematic
        # way.

        # Right now (December 2018) register_supervisor takes two
        # arguments:
        # 1. The primary execution session UUID
        # 2. One UUID, unique for each worker execution

        # Since the call to register_supervisor exists in both the
        # supervisor and the worker plan with the same argumens,
        # we can associate the two executions uniquely.

        # First get the arguments of the register_supervisor call
        for v in referenced_vars.values():
            if v['list_index'] == 1:
                supervisor_session = v['mal_value'][1:-1]
            elif v['list_index'] == 2:
                worker_uuid = v['mal_value'][1:-1]

        if json_object.get('session') == supervisor_session:
            # If we are at the supervisor execution we can find
            # the supervisor execution id.

            # First search if we have encountered the worker UUID
            # before. If yes we can resolve the data to insert
            # into the table.
            if worker_uuid in self._initiates_association:
                self._initiates_executions_id += 1
                initiates_executions_data.append({
                    "initiates_executions_id": self._initiates_executions_id,
                    "parent_id": current_execution_id,
                    "child_id": self._initiates_association[worker_uuid],
                    "remote": True,
                })
                del self._initiates_association[worker_uuid]
            else:
                # The worker UUID is not there. Make a note of the
                # association so that we are able to resolve the
                # data supervises_executions table when we
                # encounter the corresponding worker node.
                self._initiates_association[worker_uuid] = current_execution_id
        else:
            # We are at the worker execution.

            # First search if we have encountered the supervisor UUID
            # before. If yes we can resolve the data.
            if worker_uuid in self._initiates_association:
                self._initiates_executions_id += 1
                initiates_executions_data.append({
                    "initiates_executions_id": self._initiates_executions_id,
                    "parent_id": self._initiates_association[worker_uuid],
                    "child_id": current_execution_id,
                    "remote": True,
                })
                del self._initiates_association[worker_uuid]
            else:
                # Make a note of the association so that we are
                # able to resolve the data later.
                self._initiates_association[worker_uuid] = current_execution_id

    def _get_execution_id(self, session, tag):
        '''Return the (local) execution id for the given session and tag

        '''

        return self._execution_dict.get("{}:{}".format(session, tag))

    def parse_trace_stream(self, json_stream):
        '''Parse a list of json trace objects

This will create a representation ready to be inserted into the
database.
'''
        # This is a list that we use for deduplication of variables.
        var_name_list = list()
        execution = -1
        cnt = 0
        for json_event in json_stream:
            src = json_event.get("source")
            if src == "trace":
                if json_event.get('session') is None:
                    LOGGER.error(json_event)
                    raise exceptions.MalParserError('Missing session')
                elif json_event.get('tag') is None:
                    LOGGER.error(json_event)
                    raise exceptions.MalParserError('Missing tag')

                event_data, prereq_list, referenced_vars, event_variables, query_data, initiates_executions_data = self._parse_event(json_event)
                execution = self._get_execution_id(json_event.get('session'), json_event.get('tag'))
                event_data['mal_execution_id'] = execution
                # events.append(event_data)

                # Add new event to the table
                ignored_keys = ['version']
                for k, v in event_data.items():
                    if k in ignored_keys:
                        continue
                    self._events[k].append(v)


                for var_name, var in referenced_vars.items():
                    # Ignore variables that we have already seen
                    # Variables and variable names are scoped by executions
                    # (session + tag combinations). Between different
                    # executions variables with the same name are allowed to
                    # exist.
                    scoped_variable = "{}:{}".format(execution, var_name)
                    if scoped_variable in var_name_list:
                        continue

                    var_name_list.append(scoped_variable)

                    var['mal_execution_id'] = execution
                    # Add new variable to the table
                    ignored_keys = ['list_index']
                    for k, v in var.items():
                        if k in ignored_keys:
                            continue
                        self._variables[k].append(v)

                for evariable in event_variables:
                    self._event_variables['event_id'].append(evariable.get('event_id'))
                    self._event_variables['variable_list_index'].append(evariable.get('variable_list_index'))
                    # NOTE: this violates the foreign key. Why?
                    self._event_variables['variable_id'].append(evariable.get('variable_id'))


                for pev in prereq_list:
                    self._prerequisite_relation_id += 1
                    self._prerequisite_events['prerequisite_relation_id'].append(self._prerequisite_relation_id)
                    self._prerequisite_events['prerequisite_event'].append(pev)
                    self._prerequisite_events['consequent_event'].append(event_data['event_id'])

                    # prerequisite_events.append((pev, event_data['event_id']))

                if query_data is not None:
                    for k, v in query_data.items():
                        self._query.get(k).append(v)

                if initiates_executions_data is not None:
                    for i in initiates_executions_data:
                        for k, v in i.items():
                            self._initiates_executions.get(k).append(v)

                cnt += 1
        LOGGER.debug("%d JSON objects parsed", cnt)
        LOGGER.debug("initiates executions = %s", self._initiates_executions)

    def _parse_heartbeat(self, json_object):
        '''Parse a heartbeat object and adds it to the database.

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

    def get_data(self):
        """Return the data that has been parsed so far.

The data is ready to be inserted into MonetDBLite.

:returns: A dictionary, with keys the names of the tables and values
  the following dictionaries:

        - A dictionary for executions with the following keys:

          + execution_id
          + server_session
          + tag
          + server_version
          + user_function

        - A dictionary for events with the following keys:

          + event_id
          + mal_execution_id
          + pc
          + execution_state
          + clk
          + ctime
          + thread
          + mal_function
          + usec
          + rss
          + type_size
          + long_statement
          + short_statement
          + instruction
          + mal_module

        - A dictionary for prerequisite event with the following keys:

          + prerequisite_relation_id
          + prerequisite_event
          + consequent_event

        - A dictionary for variables with the following keys:

          + variable_id
          + name
          + mal_execution_id
          + alias
          + type_id
          + is_persistent
          + bid
          + var_count
          + var_size
          + seqbase
          + hghbase
          + eol
          + mal_value
          + parent

        - A dictionary for connecting events to variables with the following keys:

          + event_id
          + variable_list_index
          + variable_id

        - A dictionary for queries with the following keys:

          + query_id
          + query_text

        - A dictionary expressing the relation of execution
          supervision, with the following keys:

          + supervises_executions_id
          + supervisor_id
          + worker_id

        - A dictionary for heartbeats with the following keys:

          + heartbeat_id
          + server_session
          + clk
          + ctime
          + nvcsw

        - A dictionary for cpuloads with the following keys:

          + cpuload_id
          + heartbeat_id
          + val

        """
        if len(self._initiates_association) > 0:
            LOGGER.warning("supervisor association table not empty: %s", self._initiates_association)
        return {
            "mal_execution": self._executions,
            "profiler_event": self._events,
            "prerequisite_events": self._prerequisite_events,
            "mal_variable": self._variables,
            "event_variable_list": self._event_variables,
            "query": self._query,
            "initiates_executions": self._initiates_executions,
            "heartbeat": self._heartbeats,
            "cpuload": self._cpuloads
        }

    def clear_internal_state(self):
        """Clear the internal dictionaries.
"""
        del self._executions
        del self._events
        del self._prerequisite_events
        del self._variables
        del self._event_variables
        del self._heartbeats
        del self._cpuloads

        self._initialize_tables()


    # def parse_object(self, json_string):
    #     try:
    #         json_object = json.loads(json_string)
    #     except json.JSONDecodeError as json_error:
    #         LOGGER.warning("W001: Cannot parse object")
    #         LOGGER.warning(json_string)
    #         LOGGER.warning("Decoder reports %s", json_error)
    #         return

    #     dispatcher = {
    #         'trace': self._parse_event,
    #         'heartbeat': self._parse_heartbeat
    #     }

    #     source = json_object.get('source')
    #     if source is None:
    #         LOGGER.error("Unkown JSON object")
    #         LOGGER.error(">%s<", json_object['source'])
    #         return

    #     try:
    #         parse_func = dispatcher[source]
    #     except KeyError:
    #         # TODO raise exception?
    #         LOGGER.error("Unkown JSON object kind: %s", source)
    #         return

    #     try:
    #         parse_func(json_object)
    #     except exceptions.MalParserError as e:
    #         LOGGER.warning("Parsing JSON Object\n  %s\nfailed:\n  %s", json_object, str(e))
    #         return

    # def get_connection(self):
    #     """Get the MonetDBLite connection"""
    #     return self._connection
