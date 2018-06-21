start transaction;

drop table if exists argument_variable_list;
drop table if exists return_variable_list;
drop table if exists mal_variable;
drop table if exists prerequisite_events;
drop table if exists profiler_event;
drop table if exists mal_execution;

create table mal_execution (
       execution_id bigserial,
       server_session char(36) not null,
       tag int not null,

       unique(server_session, tag)
);

create table profiler_event (
       event_id bigserial,
       mal_execution_id bigint not null,
       pc int not null,
       is_done bool not null,
       clk bigint,
       ctime bigint,
       thread int,
       mal_function text,
       usec int,
       rss int,
       type_size int,
       long_statement text,
       short_statement text,

       foreign key (mal_execution_id) references mal_execution(execution_id),
       -- constraint unique_event
       unique(mal_execution_id, pc, is_done)
);

create table prerequisite_events (
       prerequisite_relation_id bigserial,
       prerequisite_event bigint,
       consequent_event bigint,

       -- constraint fk_prerequisite_event
       foreign key (prerequisite_event) references profiler_event(event_id),
       -- constraint fk_consequent_event
       foreign key (consequent_event) references profiler_event(event_id)
);

create table mal_variable (
       variable_id bigserial,
       name varchar(20) not null,
       mal_execution_id bigint not null,
       alias text,
       type text,  -- change this maybe?
       is_persistent bool,
       bid int,
       var_count int,
       var_size int,
       eol bool,

       foreign key (mal_execution_id) references mal_execution(execution_id),
       constraint var_unique_name unique (mal_execution_id, name)
);

create table return_variable_list (
       return_list_id bigserial,
       variable_list_index int,
       event_id bigint,
       variable_id bigint,

       foreign key (event_id) references profiler_event(event_id),
       foreign key (variable_id) references mal_variable(variable_id)
);

create table argument_variable_list (
       argument_list_id bigserial,
       variable_list_index int,
       event_id bigint,
       variable_id bigint,

       foreign key (event_id) references profiler_event(event_id),
       foreign key (variable_id) references mal_variable(variable_id)
);

commit;
