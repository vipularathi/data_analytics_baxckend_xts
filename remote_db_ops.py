import json
from datetime import datetime
from time import time, sleep

import sqlalchemy as sql
import sqlalchemy.exc as sql_exec
import pandas as pd
from sqlalchemy import insert, select
from db_config import engine_str
from common import logger, today
# from remote_db_config import engine_str

execute_retry = True
# db_name = 'data_analytics'
# pg_user = 'postgres'
# pg_pass = 'Vivek001'
# pg_host = '172.16.47.54' # or pg_host = 'localhost'
# pg_port = '5432'
# engine_str = f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:5432/{db_name}" #use

pool = sql.create_engine(engine_str, pool_size=10, max_overflow=5, pool_recycle=67, pool_timeout=30, echo=None)
conn = pool.connect()

def get_master():
    st = datetime.now()
    query = f"""
        SELECT expiry, symbol
        FROM (
            SELECT DISTINCT expiry, symbol
            FROM xts_master
            WHERE symbol IN ('NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY')
            AND (
                expiry >= current_date
                AND (
                    (EXTRACT(month FROM expiry) = EXTRACT(month FROM current_date)
                     AND EXTRACT(year FROM expiry) = EXTRACT(year FROM current_date))
                    OR
                    (EXTRACT(month FROM expiry) = mod((EXTRACT(month FROM current_date) + 1), 12)
                     AND EXTRACT(year FROM expiry) = EXTRACT(year FROM current_date) + (CASE WHEN EXTRACT(month FROM current_date) = 12 THEN 1 ELSE 0 END))
                    OR
                    (EXTRACT(month FROM expiry) = mod((EXTRACT(month FROM current_date) + 2), 12)
                     AND EXTRACT(year FROM expiry) = EXTRACT(year FROM current_date) + (CASE WHEN EXTRACT(month FROM current_date) >= 11 THEN 1 ELSE 0 END))
                )
            )
        ) AS subquery
        ORDER BY expiry
    """
    # df1 = read_sql_df(query)
    with pool.connect() as conn:
        df1 = pd.read_sql(query, conn)

    logger.info(f'Master file read from db in {(datetime.now() - st).total_seconds()} seconds')
    return df1

master_df = get_master()
# print(master_df)
