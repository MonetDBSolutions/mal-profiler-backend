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
    """A parser for the MonetDB profiler traces.

    The purpose of this class is to turn the JSON objects that the
    MonetDB profiler emmits into a representation ready to be inserted
    into a MonetDBLite-Python trace database.

    Args:
        limits: A dictionary providing the maximum values of the table
            ids currently found in the database. These include the
            following:

                * execution_id
                * event_id
                * variable_id
                * heartbeat_id
                * prereq_id
                * query_id
                * supervises_executions_id
    """

    def __init__(self, limits=dict()):
        logging.basicConfig(level=logging.DEBUG)
        self._execution_id = limits.get('max_execution_id', 0)
        self._event_id = limits.get('max_event_id', 0)
        self._variable_id = limits.get('max_variable_id', 0)
        self._heartbeat_id = limits.get('max_heartbeat_id', 0)
        self._cpuload_id = limits.get('max_cpuload_id', 0)
        self._prerequisite_relation_id = limits.get('max_prerequisite_id', 0)
        self._query_id = limits.get('max_query_id', 0)
        self._initiates_executions_id = limits.get('max_initiates_id', 0)

        self._initiates_association = dict()
        self._var_name_to_id = dict()
        self._execution_dict = dict()
        self._states = {'start': 0, 'done': 1, 'pause': 2}
        self._tables = None

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
        self._tables = dict()
        self._tables["mal_execution"] = {
            "execution_id": list(),
            "server_session": list(),
            "tag": list(),
            "server_version": list(),
            "user_function": list(),
        }

        self._tables["profiler_event"] = {
            "event_id": list(),
            "mal_execution_id": list(),
            "pc": list(),
            "execution_state": list(),
            "relative_time": list(),
            "absolute_time": list(),
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

        self._tables["prerequisite_events"] = {
            "prerequisite_relation_id": list(),
            "prerequisite_event": list(),
            "consequent_event": list(),
        }

        self._tables["mal_variable"] = {
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
            "mal_value": list(),
            "parent": list(),
        }

        self._tables["event_variable_list"] = {
            "event_id": list(),
            "variable_list_index": list(),
            "variable_id": list(),
            "created": list(),
            "eol": list(),
        }

        # BUG: If I remove query_text or root_execution_id
        # test_limits_full_db coredumps on manager.insert_data
        # (monetdblite.insert?).
        self._tables["query"] = {
            "query_id": list(),
            "query_text": list(),
            "query_label": list(),
            "root_execution_id": list(),
        }

        self._tables["initiates_executions"] = {
            "initiates_executions_id": list(),
            "parent_id": list(),
            "child_id": list(),
            "remote": list ()
        }

        self._tables["heartbeat"] = {
            "heartbeat_id": list(),
            "server_session": list(),
            "clk": list(),
            "ctime": list(),
            "rss": list(),
            "nvcsw": list(),
        }

        self._tables["cpuload"] = {
            "cpuload_id": list(),
            "heartbeat_id": list(),
            "val": list(),
        }

    def _parse_variable(self, var_data, current_execution_id):
        """Parse a single MAL variable.

        Args:
            var_data: A dictionary representing the JSON description of a MAL variable.

        Returns:
            A dictionary representing a variable. See :ref:`data_structures`.
        """
        # As mentioned elsewhere variables are scoped by
        # executions. The variable key is the concatenation of the
        # current execution id and tha variable name.
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
            "mal_value": var_data.get('value'),
            "parent": var_data.get('parent'),
            "list_index": var_data.get('index')
        }
        # If this is a new variable, add it to the variable symbol
        # table.
        if new_var:
            self._var_name_to_id[var_key] = self._variable_id

        return variable

    def _parse_query_text(self, short_description):
        """Extract the SQL executed from the short description attribute.

        Args:
            short_description: The short_description attribute of the
                JSON event.

        Returns:
            the SQL text executed.

        Note:
            A better way to accomplish this is to look up the MAL
            variable given as first argument of the querylog.define
            call and get its value. This implementation is a quick and
            dirty hack.

        """

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
        """Parse a single profiler event.

        If this is the first time we encounter this specific
        combination of ``session`` and ``tag``, create a new MAL
        execution (see :ref:`mal_execution`).

        Args:
            json_object: A dictionary representing a JSON object
            emmited by the MonetDB server.

        Returns:
            A tuple of 5 items

                * A dictionary containing the event data (see :ref:`data_structures`)
                * A list of prerequisite event ids
                * A list of referenced variables (see :ref:`data_structures`)
                * A list of argument variable ids
                * A list of return variable ids

        Raises:
            :class:`mal\_analytics.exceptions.MalParserError`: if the
                variable representation does not include a name

        """

        # Set up the execution. First get the execution id
        # corresponding to our server_session/tag combination. If it
        # is None, then set up a new execution.
        current_execution_id = self._get_execution_id(json_object.get('session'), json_object.get('tag'))
        if current_execution_id is None:
            # Get the MAL function name of this execution. This is the
            # instruction field of the event with pc == 0
            instruction = json_object.get('instruction')
            if json_object.get('pc') != 0:
                # Surprisingly this can happen! Normally the first
                # time we encounter a new combination of ``session``
                # and ``tag`` the object should have pc==0. I noticed
                # it on remote table queries. This is probably a BUG
                # in the MonetDB server.
                LOGGER.warning("Instruction with pc==0 missing for execution {}:{}".format(
                    json_object.get('session'),
                    json_object.get('tag')
                ))
                # If the event with pc == 0 does not exist, then get
                # the name by splitting the function field of the JSON
                # object.
                instruction = json_object.get('function').split('.')[1]

            # This call will not throw an exception because the
            # current_execution_id is None.
            current_execution_id = self._create_new_execution(
                json_object.get('session'),
                json_object.get('tag'),
                instruction,
                json_object.get('version')
            )

        # Fill in the event metadata
        self._event_id += 1
        event_data = {
            "event_id": self._event_id,
            "mal_execution_id": current_execution_id,
            "pc": json_object.get("pc"),
            "execution_state": self._states.get(json_object.get("state")),
            "relative_time": json_object.get("clk"),
            "absolute_time": json_object.get("ctime"),
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

        # The list of prerequisite objects is directly available from
        # the JSON object.
        prereq_list = json_object.get("prereq")

        referenced_vars = dict()
        event_variables = list()

        # The following process applies equally well to both the
        # return values and the arguments of the MAL instruction.
        for var_kind in ["ret", "arg"]:
            # We process return values only on "done" events. On
            # "start" events we are missing a lot of details (size,
            # persistent or transient, etc), that are available at
            # "done" events.
            if var_kind == "ret" and event_data['execution_state'] == self._states.get("start"):
                continue
            for item in json_object.get(var_kind, []):
                # First, parse the variable
                parsed_var = self._parse_variable(item, current_execution_id)
                var_name = parsed_var.get('name')
                if var_name is None:
                    raise exceptions.MalParserError('Unnamed variable')

                # Gather all the variables in a dictionary keyed by
                # the variable name.
                referenced_vars[var_name] = parsed_var
                # Gather some metadata about the variable.
                event_variables.append({
                    "event_id": self._event_id,
                    "variable_list_index": parsed_var.get('list_index'),
                    "variable_id": parsed_var.get('variable_id'),
                    "eol": item.get('eol') == 1 and event_data['execution_state'] == self._states.get("done"),
                    # NOTE: The following assumes that variables are
                    # created when assigned for the first time and
                    # that no more assignments are possible. Probably
                    # this assumption will be violated. We need to
                    # issue a warning when this happens, and so we
                    # need to keep track of all the variables created
                    # so far in this execution.
                    "created": var_kind == "ret",
                })

        # Handle the initiates execution relation:
        query_data = None
        initiates_executions_data = list()
        # If we are processing a querylog.define instruction, register
        # a new query and a self calling execution.
        if event_data['mal_module'] == 'querylog' and event_data['instruction'] == 'define' and event_data['execution_state'] == 0:
            query_data, new_initiates_relations = self._register_new_query(event_data, current_execution_id)
            initiates_executions_data.extend(new_initiates_relations)
        # A remote.register_supervisor instruction, signifies a remote
        # call.
        elif event_data['mal_module'] == 'remote' and event_data['instruction'] == 'register_supervisor' and event_data['execution_state'] == 0:
            self._handle_remote_initiates(json_object, referenced_vars, current_execution_id, initiates_executions_data)
        # Finally, if the mal module is "user" we are calling a user
        # defined function.
        elif event_data['mal_module'] == 'user' and event_data['execution_state'] == 0:
            # LOGGER.debug("\n  event data['instruction'] = %s\n  event_data['short_statement'] = %s\n  current execution = %d", event_data.get('instruction'), event_data.get('short_statement'), current_execution_id)
            self._handle_local_initiates(event_data, current_execution_id, initiates_executions_data)

        return (
            event_data,
            prereq_list,
            referenced_vars,
            event_variables,
            query_data,
            initiates_executions_data
        )

    def _create_new_execution(self, session, tag, user_function, server_version=None):
        """Define a new execution.

        Args:
            session: The session UUID of the execution.
            tag: The tag of the execution.
            user_function: The name of the function this execution defines.
            server_version: The MonetDB server version. This might not
                always be available.

        Returns:
            The id (a serial number) of the new execution.

        Raises:
            :class:`mal\_analytics.exceptions.MalParserError`: if
                there is already an execution with this session and
                tag.

        """
        key = "{}:{}".format(session, tag)
        execution_id = self._execution_dict.get(key)

        if execution_id is None:
            self._execution_id += 1
            execution_id = self._execution_id
            self._execution_dict[key] = execution_id

            # Add the new execution to the table.
            self._tables["mal_execution"]['execution_id'].append(execution_id)
            self._tables["mal_execution"]['server_session'].append(session)
            self._tables["mal_execution"]['tag'].append(tag)
            self._tables["mal_execution"]['user_function'].append(user_function)
            self._tables["mal_execution"]['server_version'].append(server_version)

            return execution_id
        else:
            raise exceptions.MalParserError("execution for session {}, tag {} already registered".format(session, tag))

    def _handle_local_initiates(self, event_data, current_execution_id, initiates_executions_data):
        idx = self._tables["mal_execution"]["execution_id"].index(current_execution_id)
        server_session = self._tables["mal_execution"]["server_session"][idx]

        # We are concatenating the server_session with the function
        # name. We are assuming that the function calls are local
        # within a server session. For the remote case we need to
        # detect the register_supervisor call.
        key = "{}:{}".format(server_session, event_data["instruction"])

        # We are defining a function...
        if event_data['short_statement'].startswith('function'):
            # LOGGER.debug("Defining")
            lookup_key = key + ":c"  # ...so we look up the *call* of the function
            if lookup_key in self._initiates_association:
                # LOGGER.debug("  Resolving key %s", key)
                self._initiates_executions_id += 1
                initiates_executions_data.append({
                    "initiates_executions_id": self._initiates_executions_id,
                    "parent_id": self._initiates_association[lookup_key],
                    "child_id": current_execution_id,
                    "remote": False,
                })
                del self._initiates_association[lookup_key]
            else:
                # LOGGER.debug("  Recording key %s", key)
                record_key = key + ":d"
                self._initiates_association[record_key] = current_execution_id
        else:
            # We are calling a function...
            # LOGGER.debug("Calling")
            lookup_key = key + ":d"  # ...so we look up the *definition* of the function
            if lookup_key in self._initiates_association:
                # LOGGER.debug("  Resolving key %s", key)
                self._initiates_executions_id += 1
                initiates_executions_data.append ({
                    "initiates_executions_id": self._initiates_executions_id,
                    "parent_id": current_execution_id,
                    "child_id": self._initiates_association[lookup_key],
                    "remote": False,
                })
                del self._initiates_association[lookup_key]
            else:
                # LOGGER.debug("  Recording key %s", key)
                record_key = key + ":c"
                self._initiates_association[record_key] = current_execution_id

    def _register_new_query(self, event_data, current_execution_id):
        initiates_executions_data = list()
        self._query_id += 1
        query_data = {
            "query_id": self._query_id,
            "query_text": self._parse_query_text(event_data['short_statement']),
            "query_label": None,
            "root_execution_id": current_execution_id
        }
        # LOGGER.warning("Executions: %s", self._execution_dict)
        # LOGGER.debug('Adding query {}, id: {}'.format(query_data['query_text'], query_data['query_id']))
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
        """Register or resolve a remote execution association.

        See :ref:`remote_calls` for more information.

        Args:
            json_object: The ``remote.register_supervisor``
                instruction data.
            referenced_vars: The arguments of the
                ``remote.register_supervisor`` call.
            current_execution_id: The execution id of the call. This
                is used to distinguish between caller and callee.
            initiates_executions_data: A list where the resolved
                associations are recorded.

        Todo:
            This method should be refactored to return a meanignful
            value instead of "returning" using an argument.

        """
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
        """Return the (local) execution id for the given session and tag

        """

        return self._execution_dict.get("{}:{}".format(session, tag))

    def parse_trace_stream(self, json_stream):
        """Parse a list of json trace objects

        This will create a representation ready to be inserted into
        the database. This method performs a number of actions for
        each event in the ``json_stream`` as outlined below:

        1. Basic sanity checks and initial parsing of the JSON event. At this
           stage we decide what is the execution id for this event.
        2. Handle the referenced variables.

        Args:
            json_stream: an iterable (usually a list) containing python dictionaries

        """
        # This is a list that we use for deduplication of variables.
        var_name_list = list()
        execution = -1
        cnt = 0
        for json_event in json_stream:
            # Stage 1: make sure the json_event we got from the
            # MonetDB server contains session and tag fields. These
            # fields define the MAL execution. Parse the event and
            # keep the data around for further processing.
            src = json_event.get("source")
            cnt += 1
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

                # So far, so good. Add new event to the collection
                # that needs to be inserted in the database table.
                ignored_keys = ['version']
                for k, v in event_data.items():
                    if k in ignored_keys:
                        continue
                    self._tables["profiler_event"][k].append(v)

                # Stage 2: Handle the referenced variables.
                for var_name, var in referenced_vars.items():
                    # Ignore variables that we have already seen
                    # Variables and variable names are scoped by
                    # executions (session + tag combinations). Between
                    # different executions variables with the same
                    # name are allowed to exist.
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
                        self._tables["mal_variable"][k].append(v)

                for evariable in event_variables:
                    self._tables["event_variable_list"]['event_id'].append(evariable.get('event_id'))
                    self._tables["event_variable_list"]['variable_list_index'].append(evariable.get('variable_list_index'))
                    # NOTE: this violates the foreign key. Why?
                    self._tables["event_variable_list"]['variable_id'].append(evariable.get('variable_id'))
                    self._tables["event_variable_list"]['created'].append(evariable.get('created'))
                    self._tables["event_variable_list"]['eol'].append(evariable.get('eol', False))

                for pev in prereq_list:
                    self._prerequisite_relation_id += 1
                    self._tables["prerequisite_events"]['prerequisite_relation_id'].append(self._prerequisite_relation_id)
                    self._tables["prerequisite_events"]['prerequisite_event'].append(pev)
                    self._tables["prerequisite_events"]['consequent_event'].append(event_data['event_id'])

                    # prerequisite_events.append((pev, event_data['event_id']))

                if query_data is not None:
                    for k, v in query_data.items():
                        self._tables["query"].get(k).append(v)

                if initiates_executions_data is not None:
                    for i in initiates_executions_data:
                        for k, v in i.items():
                            self._tables["initiates_executions"].get(k).append(v)

            elif src == "heartbeat":
                hb_data, cpu_data = self._parse_heartbeat(json_event)

                for k, v in hb_data.items():
                    self._tables["heartbeat"][k].append(v)

                for c in cpu_data:
                    for k, v in c.items():
                        self._tables["cpuload"][k].append(v)
            else:
                # TODO: raise exception
                pass
        LOGGER.debug("%d JSON objects parsed", cnt)
        LOGGER.debug("initiates executions = %s", self._tables["initiates_executions"])

    def _parse_heartbeat(self, json_object):
        """Parse a heartbeat object and adds it to the database.
        """
        self._heartbeat_id += 1
        # LOGGER.debug("parsing heartbeat. event id: %d", self._heartbeat_id)
        data_keys = ('clk',
                     'ctime',
                     'rss',
                     'nvcsw')
        hb_data = dict([(k, json_object.get(k)) for k in data_keys])
        hb_data['server_session'] = json_object.get('session')
        hb_data['heartbeat_id'] = self._heartbeat_id

        cpu_data = list()
        for c in json_object['cpuload']:
            self._cpuload_id += 1
            cpu_data.append({
                "cpuload_id": self._cpuload_id,
                "heartbeat_id": self._heartbeat_id,
                "val": c
            })

        return (hb_data, cpu_data)

    def get_data(self):
        """Return the data that has been parsed so far.

        The data is ready to be inserted into MonetDBLite.

        Returns:
            A dictionary, with keys the names of the tables and values
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
                  + relative_time
                  + absolute_time
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
        if self._initiates_association:
            LOGGER.warning("supervisor association table not empty: %s", self._initiates_association)
        return self._tables

    def clear_internal_state(self):
        """Clear the internal dictionaries.
        """
        self._tables = None

        self._initialize_tables()
