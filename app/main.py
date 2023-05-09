from db.conf import db_url, dwh_url
import sqlalchemy
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
from prefect import task, flow
from datetime import date, timedelta, datetime as dt


@task()
def extract_data_from_db(date: date, table_name: str, db_url: str) -> pd.DataFrame:
    sql_query = f"""
                select * from {table_name} where updated_at::date >= '{date}'::date and updated_at::date < now()::date
                """
    print(sql_query)
    with create_engine(db_url).begin() as engine:
        df = pd.read_sql(sql=sql_query, con=engine)
    return df


@flow(log_prints=True)
def extract(date: str, db_url: str = db_url) -> dict[str, pd.DataFrame]:
    print(f'Extracting data for the {date}')

    lessons_df = extract_data_from_db(
        date=date, table_name='etl_f_stream_module_lesson_view', db_url=db_url)
    print(f'Extracted lesson rows {len(lessons_df)}')

    course_df = extract_data_from_db(
        date=date, table_name='course', db_url=db_url)
    print(f'Extracted course rows {len(course_df)}')

    stream_df = extract_data_from_db(
        date=date, table_name='stream', db_url=db_url)
    print(f'Extracted stream rows {len(stream_df)}')

    stream_module_df = extract_data_from_db(
        date=date, table_name='stream_module', db_url=db_url)
    print(f'Extracted stream module rows {len(stream_module_df)}')

    return {'lesson': lessons_df,
            'course': course_df,
            'stream': stream_df,
            'stream_module': stream_module_df}

@task()
def transform(dataframes: dict):
    # just to make some transformation
    for entity, df in dataframes.items():
        index = f'{entity}_id'
        dataframes[entity] = df.rename(columns={'id': index}).set_index(index)
    dataframes['lesson']
    return dataframes


@task()
def load_to_dwh(df: pd.DataFrame, target_table: str, dwh_url: str) -> None:
    with create_engine(dwh_url).begin() as conn:
        df.to_sql(f"{target_table}", con=conn, if_exists="replace")


@task(log_prints=True)
def upsert_from_stage(dwh_url: str):
    with create_engine(dwh_url).connect() as conn:
        results = conn.execute(text('select * from upsert_all()'))
        results = results.fetchone()
        print(f"d_course: {results[0]}, d_stream: {results[1]}, d_stream_module: {results[2]}, "
              f"f_lesson: {results[3]}")
        conn.execute(text("refresh materialized view lesson_stream_dm;"))
        conn.connection.commit()


@flow(log_prints=True)
def load(dataframes: dict, dwh_url: str):
    for entity, df in dataframes.items():
        if entity == 'lesson':
            load_to_dwh(df=df, target_table='f_lesson_stage', dwh_url=dwh_url)
        else:
            load_to_dwh(df=df, target_table=f'd_{entity}_stage', dwh_url=dwh_url)
    upsert_from_stage(dwh_url)


@flow(log_prints=True)
def main(date: str = str(dt.now()-timedelta(days=1))) -> None:
    dataframes = extract(date, db_url)
    dataframes = transform(dataframes)
    load(dataframes, dwh_url)

if __name__ == '__main__':
    main()