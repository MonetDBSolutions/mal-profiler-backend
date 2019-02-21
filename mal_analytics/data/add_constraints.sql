-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

ALTER TABLE mal_execution ADD
    CONSTRAINT pk_mal_execution PRIMARY KEY (execution_id);
ALTER TABLE mal_execution ADD
    CONSTRAINT unique_me_mal_execution UNIQUE(server_session, tag);

ALTER TABLE profiler_event ADD
    CONSTRAINT pk_profiler_event PRIMARY KEY (event_id);
ALTER TABLE profiler_event ADD
    CONSTRAINT fk_pe_mal_execution_id FOREIGN KEY (mal_execution_id) REFERENCES mal_execution(execution_id);
ALTER TABLE profiler_event ADD
    CONSTRAINT unique_pe_profiler_event UNIQUE(mal_execution_id, pc, execution_state);

ALTER TABLE prerequisite_events ADD
     CONSTRAINT pk_prerequisite_events PRIMARY KEY (prerequisite_relation_id);
ALTER TABLE prerequisite_events ADD
    CONSTRAINT fk_pre_prerequisite_event FOREIGN KEY (prerequisite_event) REFERENCES profiler_event(event_id);
ALTER TABLE prerequisite_events ADD
    CONSTRAINT fk_pre_consequent_event FOREIGN KEY (consequent_event) REFERENCES profiler_event(event_id);

ALTER TABLE mal_type ADD
    CONSTRAINT pk_mal_type PRIMARY KEY (type_id);
ALTER TABLE mal_type ADD
    CONSTRAINT fk_mt_subtype_id FOREIGN KEY (subtype_id) REFERENCES mal_type(type_id);

ALTER TABLE mal_variable ADD
     CONSTRAINT pk_mal_variable PRIMARY KEY (variable_id);
ALTER TABLE mal_variable ADD
    CONSTRAINT fk_mv_mal_execution_id FOREIGN KEY (mal_execution_id) REFERENCES mal_execution(execution_id);
ALTER TABLE mal_variable ADD
    CONSTRAINT fk_mv_type_id FOREIGN KEY (type_id) REFERENCES mal_type(type_id);
ALTER TABLE mal_variable ADD
    CONSTRAINT unique_mv_var_name UNIQUE (mal_execution_id, name);

ALTER TABLE event_variable_list ADD
     CONSTRAINT pk_event_variable_list PRIMARY KEY (event_id, variable_list_index);
ALTER TABLE event_variable_list ADD
    CONSTRAINT fk_evl_event_id FOREIGN KEY (event_id) REFERENCES profiler_event(event_id);
ALTER TABLE event_variable_list ADD
    CONSTRAINT fk_evl_variable_id FOREIGN KEY (variable_id) REFERENCES mal_variable(variable_id);

ALTER TABLE query ADD
    CONSTRAINT pk_query PRIMARY KEY (query_id);
ALTER TABLE query ADD
    CONSTRAINT fk_root_execution_id FOREIGN KEY (root_execution_id) REFERENCES mal_execution(execution_id);

ALTER TABLE initiates_executions ADD
    CONSTRAINT pk_initiates_executions PRIMARY KEY (initiates_executions_id);
ALTER TABLE initiates_executions ADD
    CONSTRAINT fk_parent_id FOREIGN KEY (parent_id) REFERENCES mal_execution(execution_id);
ALTER TABLE initiates_executions ADD
    CONSTRAINT fk_child_id FOREIGN KEY (child_id) REFERENCES mal_execution(execution_id);

ALTER TABLE heartbeat ADD
    CONSTRAINT pk_heartbeat PRIMARY KEY (heartbeat_id);

ALTER TABLE cpuload ADD
    constraint fk_cl_heartbeat_id foreign key (heartbeat_id) references heartbeat(heartbeat_id);
