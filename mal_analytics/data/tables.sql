-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

start transaction;

create table mal_execution (
       execution_id bigint,
       server_session char(36) not null,
       tag int not null,
       server_version char(120),
       user_function char(30),

       constraint pk_mal_execution primary key (execution_id),
       constraint unique_me_mal_execution unique(server_session, tag)
);

create table profiler_event (
       event_id bigint,
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

       constraint pk_profiler_event primary key (event_id),
       constraint fk_pe_mal_execution_id foreign key (mal_execution_id) references mal_execution(execution_id),
       constraint unique_pe_profiler_event unique(mal_execution_id, pc, execution_state)
);

create table prerequisite_events (
       prerequisite_relation_id bigint,
       prerequisite_event bigint,
       consequent_event bigint,

       constraint pk_prerequisite_events primary key (prerequisite_relation_id),
       constraint fk_pre_prerequisite_event foreign key (prerequisite_event) references profiler_event(event_id),
       constraint fk_pre_consequent_event foreign key (consequent_event) references profiler_event(event_id)
);

create table mal_type (
       type_id int,
       tname text,
       base_size int,
       subtype_id int,

       constraint pk_mal_type primary key (type_id),
       constraint fk_mt_subtype_id foreign key (subtype_id) references mal_type(type_id)
);


create table mal_variable (
       variable_id bigint,
       name varchar(20) not null,
       mal_execution_id bigint not null,
       alias text,
       type_id int,
       is_persistent bool,
       bid int,
       var_count int,
       var_size int,
       seqbase int,
       hghbase int,
       eol bool,
       mal_value text,
       parent int,

       constraint pk_mal_variable primary key (variable_id),
       constraint fk_mv_mal_execution_id foreign key (mal_execution_id) references mal_execution(execution_id),
       constraint fk_mv_type_id foreign key (type_id) references mal_type(type_id),
       constraint unique_mv_var_name unique (mal_execution_id, name)
);

create table event_variable_list (
       event_id bigint,
       variable_list_index int,
       variable_id bigint,

       constraint pk_event_variable_list primary key (event_id, variable_list_index),
       constraint fk_evl_event_id foreign key (event_id) references profiler_event(event_id),
       constraint fk_evl_variable_id foreign key (variable_id) references mal_variable(variable_id)
);

create table query (
       query_id bigint,
       query_text text,
       query_label text,
       root_execution_id bigint,

       constraint pk_query primary key (query_id),
       constraint fk_root_execution_id foreign key (root_execution_id) references mal_execution(execution_id)
);

create table initiates_executions (
       initiates_executions_id bigint,
       parent_id bigint,
       child_id bigint,
       "remote" bool not null,

       constraint pk_initiates_executions primary key (initiates_executions_id),
       constraint fk_parent_id foreign key (parent_id) references mal_execution(execution_id),
       constraint fk_child_id foreign key (child_id) references mal_execution(execution_id)
);

create table heartbeat (
       heartbeat_id bigint,
       server_session char(36) not null,
       clk bigint,
       ctime bigint,
       rss int,
       nvcsw int,

       constraint pk_heartbeat primary key (heartbeat_id)
);

create table cpuload (
       cpuload_id bigint,
       heartbeat_id bigint,
       val decimal(3, 2),

       constraint pk_cpuload primary key (cpuload_id),
       constraint fk_cl_heartbeat_id foreign key (heartbeat_id) references heartbeat(heartbeat_id)
);
commit;


start transaction;
insert into mal_type (type_id, tname, base_size) values ( 1, 'bit', 1);
insert into mal_type (type_id, tname, base_size) values ( 2, 'bte', 1);
insert into mal_type (type_id, tname, base_size) values ( 3, 'sht', 2);
insert into mal_type (type_id, tname, base_size) values ( 4, 'int', 4);
insert into mal_type (type_id, tname, base_size) values ( 5, 'lng', 8);
insert into mal_type (type_id, tname, base_size) values ( 6, 'hge', 16);
insert into mal_type (type_id, tname, base_size) values ( 7, 'oid', 8);
insert into mal_type (type_id, tname, base_size) values ( 8, 'flt', 8);
insert into mal_type (type_id, tname, base_size) values ( 9, 'dbl', 16);
insert into mal_type (type_id, tname, base_size) values (10, 'str', -1);
insert into mal_type (type_id, tname, base_size) values (11, 'date', -1);
insert into mal_type (type_id, tname, base_size) values (12, 'void', 0);
insert into mal_type (type_id, tname, base_size) values (13, 'BAT', 0);
insert into mal_type (type_id, tname, base_size, subtype_id) values (14, 'bat[:bit]', 1, 1);
insert into mal_type (type_id, tname, base_size, subtype_id) values (15, 'bat[:bte]', 1, 2);
insert into mal_type (type_id, tname, base_size, subtype_id) values (16, 'bat[:sht]', 2, 3);
insert into mal_type (type_id, tname, base_size, subtype_id) values (17, 'bat[:int]', 4, 4);
insert into mal_type (type_id, tname, base_size, subtype_id) values (18, 'bat[:lng]', 8, 5);
insert into mal_type (type_id, tname, base_size, subtype_id) values (19, 'bat[:hge]', 16, 6);
insert into mal_type (type_id, tname, base_size, subtype_id) values (20, 'bat[:oid]', 8, 7);
insert into mal_type (type_id, tname, base_size, subtype_id) values (21, 'bat[:flt]', 8, 8);
insert into mal_type (type_id, tname, base_size, subtype_id) values (22, 'bat[:dbl]', 16, 9);
insert into mal_type (type_id, tname, base_size, subtype_id) values (23, 'bat[:str]', -1, 10);
insert into mal_type (type_id, tname, base_size, subtype_id) values (24, 'bat[:date]', -1, 11);
commit;
