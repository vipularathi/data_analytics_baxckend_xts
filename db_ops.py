import json
from datetime import datetime
from time import time, sleep

import sqlalchemy as sql
import sqlalchemy.exc as sql_exec
import pandas as pd

from common import logger, today
from db_config import engine_str, use_sqlite, s_tbl_snap, n_tbl_snap, s_tbl_opt_greeks, s_tbl_opt_straddle, \
    n_tbl_opt_straddle

execute_retry = True
pool = sql.create_engine(engine_str, pool_size=10, max_overflow=5, pool_recycle=67, pool_timeout=30, echo=None)


def insert_data(table: sql.Table, dict_data, engine_address=None, multi=False, ignore=False, truncate=False, retry=1, wait_period=5):
    """
    This is used to insert the data in dict format into the table
    :param table: SQLAlchemy Table Object
    :param dict_data: Data to be inserted
    :param engine_address: Define a custom engine string. Use default if None provided.
    :param multi: Whether to use multi query or not
    :param ignore: Use Insert Ignore while insertion
    :param truncate: Truncate table before insertion
    :param retry: Insert Retry Number
    :param wait_period: Time in seconds for retry
    :return: None
    """
    st = time()
    logger.debug(f'Data Insertion started for {table.name}')
    # engine_con_str = engine_address if engine_address is not None else engine_str
    # engine = sql.create_engine(engine_con_str)
    ins = table.insert()
    if ignore:
        if use_sqlite:
            ignore_clause = 'IGNORE' if not use_sqlite else 'OR IGNORE'
            ins = table.insert().prefix_with(ignore_clause)
        else:
            # Considering Postgres
            from sqlalchemy.dialects.postgresql import insert
            # ins = table.insert().on_conflict_do_nothing()  # Not available
            ins = insert(table).on_conflict_do_nothing()

    with pool.connect() as conn:
        if truncate:
            conn.execute(f'TRUNCATE TABLE {table.name}')
        try:
            conn.execute(ins, dict_data, multi=multi)
        except sql_exec.OperationalError as e:
            if retry > 0:
                logger.info(f"Error for {table.name}: {e}")
                logger.info(f'Retrying to insert data in {table.name} after {wait_period} seconds')
                sleep(wait_period)
                insert_data(table=table, dict_data=dict_data, engine_address=engine_address, multi=multi, ignore=ignore,
                            truncate=truncate, retry=retry-1, wait_period=wait_period)
            else:
                logger.error(f"Error for {table.name} insertion: {e}", escalate=True)
        conn.close()

    logger.debug(f"Data Inserted in {table.name} in: {time() - st} secs")


def insert_data_df(table, data: pd.DataFrame, truncate=False):
    conn = pool.connect()
    if truncate:
        conn.execute(f'TRUNCATE TABLE {table.name}')
    response = data.to_sql(table.name, con=conn, if_exists='append', index=False, method='multi')
    conn.close()
    return response


def execute_query(query, retry=2, wait_period=5, params=None):
    if params is None:
        params = {}
    st = time()
    short_query = query[:int(len(query)*0.25)]
    # logger.debug(f'Executing query...{short_query}...')
    # engine = sql.create_engine(engine_str)
    try:
        with pool.connect() as conn:
            result = conn.execute(query, params)
            # conn.execute(ins, dict_data, multi=multi)
            conn.close()
    except sql_exec.OperationalError as e:
        if retry > 0:
            logger.info(f"Error for Query {short_query}: {e}")
            logger.info(f'Retrying to execute query {short_query} after {wait_period} seconds')
            sleep(wait_period)
            result = execute_query(query=query, retry=retry-1, wait_period=wait_period)
        else:
            logger.error(f"Error for Query {short_query}: {e}", escalate=True)

    # logger.debug(f"Time taken to execute query: {time() - st} secs")
    return result


def read_sql_df(query, params=None, commit=False):
    st = time()
    # logger.debug(f"Reading query..{query[:int(len(query)*0.25)]}...")
    # engine = sql.create_engine(engine_str)
    conn = pool.connect()
    df = pd.read_sql(query, conn, params=params)
    if commit:
        conn.execute('commit')
    conn.close()
    # engine.dispose()
    # logger.debug(f'Data read in {time() - st} secs')
    return df


class DBHandler:

    """
    Meant to handle Signal related stuff only.
    """

    @classmethod
    def build_users_params(cls, users: list):
        users_in = {f"users_{_i}": _u for _i, _u in enumerate(users)}
        params_in = ",".join([f"%({i})s" for i in users_in.keys()])
        return users_in, params_in

    @classmethod
    def insert_snap_data(cls, db_data: list[dict]):
        insert_data(s_tbl_snap, db_data, ignore=True)

    @classmethod
    def get_snap_data(cls, ts: datetime):
        query = f"""
            SELECT * FROM {n_tbl_snap} WHERE "timestamp"='{ts}'
        """
        result = execute_query(query, params={'ts': ts})
        response = result.fetchall()
        return response

    @classmethod
    def insert_opt_greeks(cls, db_data: list[dict]):
        insert_data(s_tbl_opt_greeks, db_data, ignore=True)

    @classmethod
    def insert_opt_straddle(cls, db_data: list[dict]):
        insert_data(s_tbl_opt_straddle, db_data, ignore=True)

    @classmethod
    def get_straddle_minima(cls, symbol, expiry, start_from=today):
        query = f"""
            SELECT "timestamp" at time zone 'Asia/Kolkata' as ts, spot, strike, combined_premium, combined_iv, otm_iv
            FROM {n_tbl_opt_straddle}
            WHERE underlying=%(symbol)s and expiry=%(expiry)s and minima=true and "timestamp">='{start_from}';
        """
        df = read_sql_df(query, params={'symbol': symbol, 'expiry': expiry})
        return df

    @classmethod
    def get_straddle_iv_data(cls, symbol, expiry, start_from=today):
        query = f"""
                SELECT "timestamp" at time zone 'Asia/Kolkata' as ts, spot, strike, combined_premium, combined_iv, otm_iv, minima
                FROM {n_tbl_opt_straddle}
                WHERE underlying=%(symbol)s and expiry=%(expiry)s and "timestamp">='{start_from}';
            """
        df = read_sql_df(query, params={'symbol': symbol, 'expiry': expiry})
        return df
