-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

start transaction;

drop table if exists argument_variable_list;
drop table if exists return_variable_list;
drop table if exists mal_variable;
drop table if exists mal_type;
drop table if exists prerequisite_events;
drop table if exists profiler_event;
drop table if exists mal_execution;

create table mal_execution (
       execution_id bigserial,
       server_session char(36) not null,
       tag int not null,

       constraint unique_me_mal_execution unique(server_session, tag)
);

create table profiler_event (
       event_id bigserial,
       mal_execution_id bigint not null,
       pc int not null,
       execution_state tinyint not null,
       clk bigint,
       ctime bigint,
       thread int,
       mal_function text,
       usec int,
       rss int,
       type_size int,
       long_statement text,
       short_statement text,
       instruction text,
       mal_module text,

       constraint fk_pe_mal_execution_id foreign key (mal_execution_id) references mal_execution(execution_id),
       -- constraint unique_event
       constraint unique_pe_profiler_event unique(mal_execution_id, pc, execution_state)
);

create table prerequisite_events (
       prerequisite_relation_id bigserial,
       prerequisite_event bigint,
       consequent_event bigint,

       constraint fk_pre_prerequisite_event foreign key (prerequisite_event) references profiler_event(event_id),
       constraint fk_pre_consequent_event foreign key (consequent_event) references profiler_event(event_id)
);

create table mal_type (
       type_id serial,
       tname text,
       base_size int,
       subtype_id int,

       constraint fk_mt_subtype_id foreign key (subtype_id) references mal_type(type_id)
);


create table mal_variable (
       variable_id bigserial,
       name varchar(20) not null,
       mal_execution_id bigint not null,
       alias text,
       type_id int,  -- change this maybe?
       is_persistent bool,
       bid int,
       var_count int,
       var_size int,
       seqbase int,
       hghbase int,
       eol bool,
       mal_value text,

       constraint fk_mv_mal_execution_id foreign key (mal_execution_id) references mal_execution(execution_id),
       constraint fk_mv_type_id foreign key (type_id) references mal_type(type_id),
       constraint unique_mv_var_name unique (mal_execution_id, name)
);

create table return_variable_list (
       return_list_id bigserial,
       variable_list_index int,
       event_id bigint,
       variable_id bigint,

       constraint fk_rv_event_id foreign key (event_id) references profiler_event(event_id),
       constraint fk_rv_variable_id foreign key (variable_id) references mal_variable(variable_id)
);

create table argument_variable_list (
       argument_list_id bigserial,
       variable_list_index int,
       event_id bigint,
       variable_id bigint,

       constraint fk_av_event_id foreign key (event_id) references profiler_event(event_id),
       constraint fk_av_variable_id foreign key (variable_id) references mal_variable(variable_id)
);

commit;

start transaction;
drop table if exists cpuload;
drop table if exists heartbeat;

create table heartbeat (
       heartbeat_id bigserial,
       server_session char(36) not null,
       clk bigint,
       ctime bigint,
       rss int,
       -- Non voluntary context switch
       nvcsw int
);

create table cpuload (
       cpuload_id bigserial,
       heartbeat_id bigint,
       val decimal(3, 2),

       constraint fk_cl_heartbeat_id foreign key (heartbeat_id) references heartbeat(heartbeat_id)
);
commit;


start transaction;
insert into mal_type (tname, base_size) values ('bit', 1);
insert into mal_type (tname, base_size) values ('bte', 1);
insert into mal_type (tname, base_size) values ('sht', 2);
insert into mal_type (tname, base_size) values ('int', 4);
insert into mal_type (tname, base_size) values ('lng', 8);
insert into mal_type (tname, base_size) values ('hge', 16);
insert into mal_type (tname, base_size) values ('oid', 8);
insert into mal_type (tname, base_size) values ('flt', 8);
insert into mal_type (tname, base_size) values ('dbl', 16);
insert into mal_type (tname, base_size) values ('str', -1);
insert into mal_type (tname, base_size) values ('date', -1);
insert into mal_type (tname, base_size) values ('void', 0);
insert into mal_type (tname, base_size) values ('BAT', 0);
insert into mal_type (tname, base_size, subtype_id) values ('bat[:bit]', 1, 1);
insert into mal_type (tname, base_size, subtype_id) values ('bat[:bte]', 1, 2);
insert into mal_type (tname, base_size, subtype_id) values ('bat[:sht]', 2, 3);
insert into mal_type (tname, base_size, subtype_id) values ('bat[:int]', 4, 4);
insert into mal_type (tname, base_size, subtype_id) values ('bat[:lng]', 8, 5);
insert into mal_type (tname, base_size, subtype_id) values ('bat[:hge]', 16, 6);
insert into mal_type (tname, base_size, subtype_id) values ('bat[:oid]', 8, 7);
insert into mal_type (tname, base_size, subtype_id) values ('bat[:flt]', 8, 8);
insert into mal_type (tname, base_size, subtype_id) values ('bat[:dbl]', 16, 9);
insert into mal_type (tname, base_size, subtype_id) values ('bat[:str]', -1, 10);
insert into mal_type (tname, base_size, subtype_id) values ('bat[:date]', -1, 11);

commit;
