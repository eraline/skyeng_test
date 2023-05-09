from conf import dwh_url, db_url
from schemes import DB_QUERY, DWH_QUERY
from sqlalchemy import engine, text


def main():
    db_engine = engine.create_engine(db_url)
    dwh_engine = engine.create_engine(dwh_url)

    with db_engine.begin() as conn:
        conn.execute(text(DB_QUERY))

    with dwh_engine.begin() as conn:
        conn.execute(text(DWH_QUERY))


if __name__ == '__main__':
    main()