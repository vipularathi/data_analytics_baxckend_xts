from datetime import datetime

import sqlalchemy as sql
from sqlalchemy import MetaData, Table, Column, Integer, DateTime, DECIMAL, VARCHAR, TEXT, Index, UniqueConstraint, \
    func, BOOLEAN, create_engine, Date, ForeignKey, Enum, Time, Float, text
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP

from common import today


use_sqlite = False  # Used in Table DDL as well
rdbms_type = 'postgres'

# db_name = f'data_{today.strftime(dt_fmt_1)}'
db_name = f'data_arathi'
pg_user = 'postgres'
pg_pass = 'root'
pg_host = '172.16.47.81'
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
    UniqueConstraint('timestamp', name=f'uq_{n_tbl_snap}_record')
)

n_tbl_opt_greeks = 'opt_greeks'
s_tbl_opt_greeks = Table(
    n_tbl_opt_greeks, metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('timestamp', TIMESTAMP(True), nullable=False),
    Column('symbol', VARCHAR(50), nullable=False),
    Column('underlying', VARCHAR(50), nullable=False),
    Column('expiry', Date, nullable=False),
    Column('strike', Float, nullable=False),
    Column('opt', VARCHAR(20), nullable=False),
    Column('ltp', Float),
    Column('spot', Float),
    Column('oi', Integer),
    Column('iv', Float),
    Column('delta', Float),
    Column('theta', Float),
    Column('gamma', Float),
    Column('vega', Float),
    Column('rho', Float),
    UniqueConstraint('timestamp', 'symbol', 'underlying', 'expiry', 'strike', name=f'uq_{n_tbl_opt_greeks}_record')
)

n_tbl_opt_straddle = 'opt_straddle'
s_tbl_opt_straddle = Table(
    n_tbl_opt_straddle, metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('timestamp', TIMESTAMP(True), nullable=False),
    Column('underlying', VARCHAR(50), nullable=False),
    Column('expiry', Date, nullable=False),
    Column('strike', Float, nullable=False),
    Column('call', VARCHAR(50), nullable=True),
    Column('put', VARCHAR(50), nullable=True),
    Column('spot', Float),
    Column('call_price', Float),
    Column('put_price', Float),
    Column('call_oi', Integer),
    Column('put_oi', Integer),
    Column('call_iv', Float),
    Column('put_iv', Float),
    Column('combined_premium', Float),
    Column('combined_iv', Float),
    Column('otm_iv', Float),
    Column('minima', BOOLEAN),
    UniqueConstraint('timestamp', 'underlying', 'expiry', 'strike', name=f'uq_{n_tbl_opt_straddle}_record')
)

n_tbl_chart_users = 'chart_users'
s_tbl_chart_users = Table(
    n_tbl_chart_users, metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('email', VARCHAR(50), nullable=False),
    Column('password', VARCHAR(50), nullable=False),
    UniqueConstraint('email', 'password', name=f'uq_{n_tbl_chart_users}_record')
)

n_tbl_master = 'xts_master'
s_tbl_master = Table(
    n_tbl_master, metadata,
    Column('scripcode', Integer),
    Column('exchange', Integer),
    Column('symbol', VARCHAR(50), nullable=False),
    Column('name', VARCHAR(50), nullable=False),
    Column('opt', VARCHAR(20)),
    Column('expiry', Date, nullable=False),
    Column('strike', Float),
    Column('opt_type', VARCHAR(20)),
    UniqueConstraint('symbol','name','expiry', name=f'uq_{n_tbl_master}_record')
)

n_tbl_creds = 'creds'
s_tbl_creds = Table(
    n_tbl_creds, metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('appkey', VARCHAR(50), nullable=True),
    Column('secretkey', VARCHAR(50), nullable=True),
    Column('userid', VARCHAR(50), nullable=True),
    Column('token', VARCHAR(255), nullable=True),
    Column('status', VARCHAR(20), nullable=True)
)

# Last and after all table declarations
# noinspection PyUnboundLocalVariable
meta_engine = sql.create_engine(engine_str)
metadata.create_all(meta_engine)
meta_engine.dispose()
