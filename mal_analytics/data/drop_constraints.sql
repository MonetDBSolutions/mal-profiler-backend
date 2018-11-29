-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

ALTER TABLE mal_execution
    DROP CONSTRAINT unique_me_mal_execution;
ALTER TABLE profiler_event
    DROP CONSTRAINT fk_pe_mal_execution_id;
ALTER TABLE profiler_event
    DROP CONSTRAINT unique_pe_profiler_event;
ALTER TABLE prerequisite_events
    DROP CONSTRAINT fk_pre_prerequisite_event;
ALTER TABLE prerequisite_events
    DROP CONSTRAINT fk_pre_consequent_event;
ALTER TABLE mal_type
    DROP CONSTRAINT fk_mt_subtype_id;
ALTER TABLE mal_variable
    DROP CONSTRAINT fk_mv_mal_execution_id;
ALTER TABLE mal_variable
    DROP CONSTRAINT fk_mv_type_id;
ALTER TABLE mal_variable
    DROP CONSTRAINT unique_mv_var_name;
ALTER TABLE return_variable_list
    DROP CONSTRAINT fk_rv_event_id;
ALTER TABLE return_variable_list
    DROP CONSTRAINT fk_rv_variable_id;
ALTER TABLE argument_variable_list
    DROP CONSTRAINT fk_av_event_id;
ALTER TABLE argument_variable_list
    DROP CONSTRAINT fk_av_variable_id;
ALTER TABLE cpuload
    DROP CONSTRAINT fk_cl_heartbeat_id;