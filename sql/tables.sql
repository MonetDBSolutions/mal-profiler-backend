create table profiler_event (
       event_id bigserial,
       server_session char(36) not null,
       tag int not null,
       pc int not null,
       is_done bool not null,
       clk bigint,
       ctime bigint,
       thread int,
       func text,
       usec int,
       rss int,
       type_size int,
       long_statement text,
       short_statement text,

       constraint unique_event unique(server_session, tag, pc, is_done)
);

create table prerequisites (
       prerequisite_relation_id bigserial,
       prerequisite_event bigint,
       consequent_event bigint,

       constraint fk_prerequisite_event foreign key (prerequisite_event) references profiler_event(event_id),
       constraint fk_consequent_event foreign key (consequent_event) references profiler_event(event_id)
);
