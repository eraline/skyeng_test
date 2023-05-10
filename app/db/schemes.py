DB_QUERY="""
drop table if exists stream_module_lesson cascade;
drop table if exists stream_module cascade;
drop table if exists stream cascade;
drop table if exists course cascade;

create table course
(
   id                    integer      primary key,
   title                 varchar(255),
   created_at            timestamp,
   updated_at            timestamp,
   deleted_at            timestamp(0),
   icon_url              varchar(255),
   is_auto_course_enroll boolean,
   is_demo_enroll        boolean
);


insert into course values
	(1, 'Python development', now() - interval '1 day', now() - interval '1 day', null, null, true, false),
	(2, 'Java development', now() - interval '2 day', now() - interval '1 day', null, null, true, false),
	(3, 'C# development', now() - interval '1 day', now() - interval '1 day', null, null, false, false),
	(4, 'Marketing', now() - interval '1 day', now() - interval '1 day', null, null, true, true);



create table stream
(
   id                     integer primary key,
   course_id              integer references course(id),
   start_at               timestamp,
   end_at                 timestamp,
   created_at             timestamp,
   updated_at             timestamp,
   deleted_at             timestamp,
   is_open                boolean,
   name                   varchar(255),
   homework_deadline_days integer
);

insert into stream values
	(1, 1, now(), now() + interval '5 month', now() - interval '1 day', now() - interval '1 day', null, true, 'Stream Python 1', 5),
	(2, 1, now() + interval '1 month', now() + interval '6 month', now() - interval '1 day', now() - interval '1 day', null, true, 'Stream Python 2', 5),
	(3, 2, now(), now() + interval '5 month', now() - interval '1 day', now() - interval '1 day', null, true, 'Stream Java 1 ', 5),
	(4, 2, now() + interval '1 month', now() + interval '6 month', now() - interval '1 day', now() - interval '1 day', null, true, 'Stream Java 2', 5);

create table stream_module
(
   id  integer primary key,
   stream_id       integer references stream(id),
   title           varchar(255),
   created_at      timestamp,
   updated_at      timestamp,
   order_in_stream integer,
   deleted_at      timestamp
);

insert into stream_module values 
	(1, 1, 'Django backend', now() - interval '1 day', now() - interval '1 day', 1, null),
	(2, 1, 'DRF', now() - interval '1 day', now() - interval '1 day', 2, null),
	(3, 1, 'Algs', now() - interval '1 day', now() - interval '1 day', 3, null),
	(4, 3, 'Java essentials', now() - interval '1 day', now() - interval '1 day', 1, null),
	(5, 3, 'Java advanced', now() - interval '1 day', now() - interval '1 day', 2, null);

create table stream_module_lesson
(
   id                          integer primary key,
   title                       varchar(255),
   description                 text,
   start_at                    timestamp,
   end_at                      timestamp,
   homework_url                varchar(500),
   teacher_id                  integer,
   stream_module_id            integer references stream_module(id),
   deleted_at                  timestamp(0),
   online_lesson_join_url      varchar(255),
   online_lesson_recording_url varchar(255)
);

insert into stream_module_lesson values
	(1, 'Lesson 1', 'Lesson 1', now(), now(), null, 1, 1, null, null, null),
	(2, 'Lesson 2', 'Lesson 2', now(), now(), null, 1, 1, null, null, null),
	(3, 'Lesson 3', 'Lesson 3', now(), now(), null, 1, 1, null, null, null),
	(4, 'Lesson 1', 'Lesson 1', now(), now(), null, 2, 2, null, null, null),
	(5, 'Lesson 2', 'Lesson 2', now(), now(), null, 2, 2, null, null, null),
	(6, 'Lesson 3', 'Lesson 3', now(), now(), null, 2, 2, null, null, null),
	(7, 'Lesson 1', 'Lesson 1', now(), now(), null, 2, 4, null, null, null),
	(8, 'Lesson 2', 'Lesson 2', now(), now(), null, 2, 4, null, null, null),
	(9, 'Lesson 3', 'Lesson 3', now(), now(), null, 2, 4, null, null, null),
	(10, 'Lesson 1', 'Lesson 1', now(), now(), null, 3, 5, null, null, null),
	(11, 'Lesson 2', 'Lesson 2', now(), now(), null, 3, 5, null, null, null),
	(12, 'Lesson 3', 'Lesson 3', now(), now(), null, 3, 5, null, null, null);


drop view if exists etl_f_stream_module_lesson_view cascade;
create or replace view etl_f_stream_module_lesson_view as
select sml.id, 
	   sml.title, 
	   sml.description, 
	   sml.start_at, 
	   sml.end_at, 
	   sml.homework_url,
	   sml.teacher_id,
	   sml.stream_module_id,
	   sml.deleted_at,
	   sml.online_lesson_join_url,
	   sml.online_lesson_recording_url,
	   sm.stream_id,
	   s.course_id,
	   sm.updated_at::date as updated_at,
	   sm.created_at::date as created_at
	   from stream_module_lesson sml
	   join stream_module sm on sml.stream_module_id = sm.id 
	   join stream s on sm.stream_id = s.id ;
"""


DWH_QUERY = """
drop table if exists d_course CASCADE;
drop table if exists d_stream CASCADE;
drop table if exists d_stream_module CASCADE;
drop table if exists f_lesson CASCADE;


CREATE TABLE d_course (
    course_id INT PRIMARY KEY,
    title VARCHAR(255),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    deleted_at TIMESTAMP(0),
    icon_url VARCHAR(255),
    is_auto_course_enroll BOOLEAN,
    is_demo_enroll BOOLEAN
);

CREATE TABLE d_stream (
    stream_id INT PRIMARY KEY,
    start_at TIMESTAMP,
    end_at TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    deleted_at TIMESTAMP,
    is_open BOOLEAN,
    name VARCHAR(255),
    homework_deadline_days INT
);

CREATE TABLE d_stream_module (
    stream_module_id INT PRIMARY KEY,
    title VARCHAR(255),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    order_in_stream INT,
    deleted_at TIMESTAMP
);

CREATE TABLE f_lesson (
    id INT PRIMARY KEY,
    stream_module_id INT REFERENCES d_stream_module(stream_module_id),
    course_id INT REFERENCES d_course(course_id),
    stream_id INT REFERENCES d_stream(stream_id),
    title VARCHAR(255),
    description TEXT,
    start_at TIMESTAMP,
    end_at TIMESTAMP,
    homework_url VARCHAR(500),
    teacher_id INT,
    deleted_at TIMESTAMP,
    online_lesson_join_url VARCHAR(255),
    online_lesson_recording_url VARCHAR(255)
);

CREATE OR REPLACE FUNCTION upsert_d_course() 
RETURNS table(row_cnt integer) as
$$
BEGIN
    INSERT INTO d_course (course_id, title, created_at, updated_at, deleted_at, icon_url, is_auto_course_enroll, is_demo_enroll)
        SELECT course_id, title, created_at, updated_at, cast(deleted_at as timestamp) as deleted_at, icon_url, is_auto_course_enroll, is_demo_enroll FROM d_course_stage
        ON CONFLICT (course_id) DO 
        UPDATE SET 
            title = EXCLUDED.title,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at,
            deleted_at = cast(excluded.deleted_at as timestamp),
            icon_url = EXCLUDED.icon_url,
            is_auto_course_enroll = EXCLUDED.is_auto_course_enroll,
            is_demo_enroll = EXCLUDED.is_demo_enroll;
    get diagnostics row_cnt = ROW_COUNT;
    RETURN query select row_cnt;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION upsert_d_stream() 
RETURNS TABLE (row_cnt INTEGER) AS $$
BEGIN
    INSERT INTO d_stream (stream_id, start_at, end_at, created_at, updated_at, deleted_at, is_open, name, homework_deadline_days)
        SELECT stream_id, start_at, end_at, created_at, updated_at, cast(deleted_at as timestamp) as deleted_at, is_open, name, homework_deadline_days
        FROM d_stream_stage
        ON CONFLICT (stream_id) DO UPDATE
        SET start_at = excluded.start_at,
            end_at = excluded.end_at,
            created_at = excluded.created_at,
            updated_at = excluded.updated_at,
            deleted_at = cast(excluded.deleted_at as timestamp),
            is_open = excluded.is_open,
            name = excluded.name,
            homework_deadline_days = excluded.homework_deadline_days;
    get diagnostics row_cnt = ROW_COUNT;
    RETURN query select row_cnt;
   END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION upsert_d_stream_module() 
RETURNS TABLE(row_cnt int) AS
$$
BEGIN
    INSERT INTO d_stream_module (stream_module_id, title, created_at, updated_at, order_in_stream, deleted_at)
        SELECT stream_module_id, title, created_at, updated_at, order_in_stream, cast(deleted_at as timestamp) as deleted_at FROM d_stream_module_stage
        ON CONFLICT (stream_module_id) DO UPDATE SET 
            title = EXCLUDED.title,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at,
            order_in_stream = EXCLUDED.order_in_stream,
            deleted_at = cast(excluded.deleted_at as timestamp);
    get diagnostics row_cnt = ROW_COUNT;
    RETURN query select row_cnt;
   END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION upsert_f_lesson()
RETURNS INT AS $$
DECLARE
  inserted_rows INT;
BEGIN
  INSERT INTO f_lesson
    SELECT fls.lesson_id,
    			fls.stream_module_id,
    			fls.course_id,
    			fls.stream_id,
    			fls.title,
    			fls.description,
    			fls.start_at,
    			fls.end_at,
    			fls.homework_url,
    			fls.teacher_id,
    			cast(fls.deleted_at as timestamp) as deleted_at,
    			fls.online_lesson_join_url,
    			fls.online_lesson_recording_url 
    	FROM f_lesson_stage fls
    ON CONFLICT (id) DO UPDATE
      SET stream_module_id = excluded.stream_module_id,
          course_id = excluded.course_id,
          stream_id = excluded.stream_id,
          title = excluded.title,
          description = excluded.description,
          start_at = excluded.start_at,
          end_at = excluded.end_at,
          homework_url = excluded.homework_url,
          teacher_id = excluded.teacher_id,
          deleted_at = cast(excluded.deleted_at as timestamp),
          online_lesson_join_url = excluded.online_lesson_join_url,
          online_lesson_recording_url = excluded.online_lesson_recording_url;

  GET DIAGNOSTICS inserted_rows = ROW_COUNT;

  RETURN inserted_rows;
END;
$$ LANGUAGE plpgsql;

create or replace function upsert_all()
RETURNS TABLE(affected_rows_course int, 
			  affected_rows_stream int,
			  affected_rows_stream_module int,
			  affected_rows_lesson int) AS $$
begin
  return query select upsert_d_course(),
 				upsert_d_stream(),
 				upsert_d_stream_module(),
 				upsert_f_lesson();
END;
$$ LANGUAGE plpgsql;

drop view if exists lesson_stream_dm;
create materialized view lesson_stream_dm
as
select 
	fls.id, 
	fls.title as lesson_title, 
	fls.teacher_id, 
	fls.stream_module_id, 
	fls.stream_id, ds."name", 
	ds.homework_deadline_days, 
	fls.course_id,
	dc.title as course_title,
	dc.is_demo_enroll
from 
f_lesson fls
join d_stream ds using (stream_id)
join d_course dc using (course_id);

"""