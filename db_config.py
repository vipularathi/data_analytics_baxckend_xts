from datetime import datetime

import sqlalchemy as sql
from sqlalchemy import MetaData, Table, Column, Integer, DateTime, DECIMAL, VARCHAR, TEXT, Index, UniqueConstraint, \
    func, BOOLEAN, create_engine, Date, ForeignKey, Enum, Time
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP

from common import today


use_sqlite = False  # Used in Table DDL as well
rdbms_type = 'postgres'

# db_name = f'data_{today.strftime(dt_fmt_1)}'
db_name = f'data_arathi'
pg_user = 'postgres'
pg_pass = '123456'
pg_host = 'localhost'
engine_str = f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:5432/{db_name}"
temp_engine_str = f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:5432"
with create_engine(temp_engine_str, isolation_level='AUTOCOMMIT').connect() as conn:
    res = conn.execute(f"select * from pg_database where datname='{db_name}';")
    rows = res.rowcount > 0
    if not rows:
        # conn.execute('commit')
        res_db = conn.execute(f'CREATE DATABASE {db_name};')
        print(f"DB created {db_name}. Response: {res_db.rowcount}")

metadata = MetaData()

n_tbl_snap = 'snap'
s_tbl_snap = Table(
    n_tbl_snap, metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('timestamp', TIMESTAMP(True), nullable=False),
    Column('snap', JSONB, nullable=False),
    Column('created_at', TIMESTAMP(True), server_default=func.current_timestamp()),
    Column('updated_at', TIMESTAMP(True), server_default=func.current_timestamp()),
)


# Last and after all table declarations
# noinspection PyUnboundLocalVariable
meta_engine = sql.create_engine(engine_str)
metadata.create_all(meta_engine)
meta_engine.dispose()
