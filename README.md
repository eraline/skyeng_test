# Тестовое задание для SkyEng Data Engineer Role

## Запуск Проекта

Для запуска проекта необходимо склонировать репозиторий и из корневой папки запустить следующую команду:

```bash
  docker-compose up
```
После выполнения команды будут созданы обычная БД, DWH заполнены тестовые данные, запуститься ETL для изначальных данных и создатся расписание для запуска ETL на ежедневной основе для данных за "вчера".

Prefect UI будет доступен по ссылке:
http://127.0.0.1:4200
База данных доступна по порту 5433
DWH доступен по порту 5434

## Отчет по заданию

### Проектирование модели и построение ER-Диаграммы :
Изначальная схема БД:

![Database model](db_model.png)

Для DWH я решил денормализовать БД по модели Star Schema.
Таблиц не так много, за основную таблицу фактов я взял сущность stream_module_lesson и добавил к ней информацию о потоках и курсе, в рамках которых проходят данные уроки. Таким образом в одной лишь таблице фактов у нас хранятся все данные, а в случае необходимости дополнительных данных из таблиц course, stream и stream_module понадобится только один join. 
Получилась следущая модель:
![Database model](dwh.png)

### Построение ETL:

Для реализации ETL я решил для удобства создать view в БД источнике, на основе которой соберутся данные для целевой таблицы в DWH:
```SQL
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
	   join stream s on sm.stream_id = s.id;
```
Затем Prefect flow написанный на python [ETL:main.py](app/main.py) выгружает данные из таблиц course, stream, stream_module и view etl_f_stream_module_lesson_view и создает 4 ДатаФрейма. После этого происходит Transform часть и переименовываются некоторые поля.

Далее выполняется Load часть:
В рамках процесса данные из всех датафреймов выгружаются в **Stage** таблицы (d_stream_module_stage, d_course_stage, d_stream_stage и f_lesson_stage).
После загрузки в промежуточные таблицы вызываются хранимые в Postgres функции, которые выполняют операцию UPSERT (Insert and Update) для всех новых данных. Пример функции для Upsert'a данных по курсам:

```SQL
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
```

Код всех процедур можно найти тут: [ссылка](https://github.com/eraline/skyeng_test/blob/c8abd5e63bc808acf476b5e785895829db241f7b/app/db/schemes.py#L174)

В конце выполнения ETL запускается refresh для витрины данных, о котором поговорим ниже.

Данный Flow (ETL) работает ежедневно по расписанию, запускается и логгируется с помощью утилиты Prefect, на скриншоте видно, что ETL успешно отработал и следующий в статусе "scheduled"
![prefect ui](prefect_ui.png)

### Витрина данных
Для витрины данных я создал материализованное представление, т.к. оно кэширует результат, а данные будут менятся лишь раз в день, поэтому по окончании выгрузки материализованное представление автоматически обновляется учитвая последние загруженные данные. Таким образом оно будет быстро работать, не нагружать систему и всегда иметь самые актуальные данные.
Код витрины данных:

```SQL
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
f_lesson fl fl
join d_stream ds using (stream_id)
join d_course dc using (course_id);
```
