-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

ALTER TABLE mal_execution ADD
    CONSTRAINT unique_me_mal_execution UNIQUE(server_session, tag);
ALTER TABLE profiler_event ADD
    CONSTRAINT fk_pe_mal_execution_id FOREIGN KEY (mal_execution_id) REFERENCES mal_execution(execution_id);
ALTER TABLE profiler_event ADD
    CONSTRAINT unique_pe_profiler_event UNIQUE(mal_execution_id, pc, execution_state);
ALTER TABLE prerequisite_events ADD
    constraint fk_pre_prerequisite_event foreign key (prerequisite_event) references profiler_event(event_id);
ALTER TABLE prerequisite_events ADD
    constraint fk_pre_consequent_event foreign key (consequent_event) references profiler_event(event_id);
ALTER TABLE mal_type ADD
    constraint fk_mt_subtype_id foreign key (subtype_id) references mal_type(type_id);
ALTER TABLE mal_variable ADD
    constraint fk_mv_mal_execution_id foreign key (mal_execution_id) references mal_execution(execution_id);
ALTER TABLE mal_variable ADD
    constraint fk_mv_type_id foreign key (type_id) references mal_type(type_id);
ALTER TABLE mal_variable ADD
    constraint unique_mv_var_name unique (mal_execution_id, name);
ALTER TABLE return_variable_list ADD
    constraint fk_rv_event_id foreign key (event_id) references profiler_event(event_id);
ALTER TABLE return_variable_list ADD
    constraint fk_rv_variable_id foreign key (variable_id) references mal_variable(variable_id);
ALTER TABLE argument_variable_list ADD
    constraint fk_av_event_id foreign key (event_id) references profiler_event(event_id);
ALTER TABLE argument_variable_list ADD
    constraint fk_av_variable_id foreign key (variable_id) references mal_variable(variable_id);
ALTER TABLE cpuload ADD
    constraint fk_cl_heartbeat_id foreign key (heartbeat_id) references heartbeat(heartbeat_id);
