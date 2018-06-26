# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import json
import logging
import monetdblite as mdbl


LOGGER = logging.getLogger(__name__)


class ProfilerObjectParser:
    def __init__(self):
        self._event_id = 0
        self._heartbeat_id = 0
        self._execution_id = 0
        self._variable_id = 0

    def parse_object(self, json_string):
        try:
            json_object = json.loads(json_string)
        except json.JSONDecodeError as json_error:
            LOGGER.warning("W001: Cannot parse object")
            LOGGER.warning(json_string)
            LOGGER.warning("Decoder reports %s", json_string)
            return

        source = json_object.get('source')
        if source is None:
            LOGGER.error("Unkown JSON object")
            LOGGER.error("%s", json_object['source'])
            return

        if source == 'trace':
            self._execution_id += 1
            self._event_id += 1
            # Extract the mal execution
            execution_data = {
                'server_session': json_object.get('session'),
                'tag':  json_object.get('tag')
            }
            mdbl.insert('mal_execution', execution_data)

            is_done = (True if json_object.get('state') == 'done' else False)
            event_data = {
                'mal_execution_id': self._execution_id,
                'pc': json_object.get('pc'),
                'is_done': is_done,
                'clk': json_object.get('clk'),
                'ctime': json_object.get('ctime'),
                'thread': json_object.get('thread'),
                'mal_function': json_object.get('function'),
                'usec': json_object.get('usec'),
                'rss': json_object.get('rss'),
                'type_size': json_object.get('size'),
                'long_statement': json_object.get('stmt'),
                'short_statement': json_object.get('short')
            }
            mdbl.insert('profiler_event', event_data)

            # Process prerequisite events.
            for prereq in json_object.get('prereq'):
                mdbl.insert('prerequisite_events', {
                    'prerequisite_event': prereq,
                    'consequent_event': self._event_id
                })

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
                    # FIXME: Use prepared statements
                    # Have we encountered this variable before?
                    r = mdbl.sql('select variable_id from mal_variable where name={}'.format(var['name']))
                    if len(r['variable_id']) == 0:
                        # Nope, first time we see this
                        # variable. Insert it into the variables
                        # table.
                        self._variable_id += 1
                        is_persistent = True if var.get('kind') == 'persistent' else False
                        variable_data = {
                            'name': var.get('name'),
                            'mal_execution_id': self._execution_id,
                            'alias': var.get('alias'),
                            'type': var.get('type'),
                            'is_persistent': is_persistent,
                            'bid': var.get('bid'),
                            'var_count': var.get('count'),
                            'var_size': var.get('size'),
                            'seqbase': var.get('seqbase'),
                            'hghbase': var.get('hghbase'),
                            'eol': True if var.get('eol') == 0 else False
                        }
                        mdbl.insert('mal_variable', variable_data)
                        current_var_id = self._variable_id
                    else:
                        # Yup, make a note of the variable id.
                        current_var_id = r['variable_id'][0]

                    var_list_data = {
                        'variable_list_index': var.get('index'),
                        'event_id': self._event_id,
                        'variable_id': current_var_id
                    }

                    mdbl.insert(var_list_tables[var_list_field], var_list_data)

        elif source == 'heartbeat':
            self._heartbeat_id += 1
            data_keys = ('server_session',
                         'clk',
                         'ctime',
                         'rss',
                         'nvcsw')
            data = {(k, json_object.get(k)) for k in data_keys}
            mdbl.insert('heartbeat', data)
            for c in json_object['cpuload']:
                mdbl.insert('cpuload',
                            {'heartbeat_id': self._heartbeat_id,
                             'val': c})
        else:
            # TODO raise exception
            LOGGER.error("Unkown JSON object kind: %s", json_object['source'])
            return
