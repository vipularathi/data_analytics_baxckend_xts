import json
from datetime import datetime

import pandas as pd

from analysis import SnapAnalysis
from common import IST, logger
from contracts import get_req_contracts
from db_ops import DBHandler


def ts_snap(group_df: pd.DataFrame):
    result = {}
    for _row in group_df.itertuples():
        ltp = _row.open
        oi = _row.oi
        result[_row.entity] = {'last_price': None if pd.isna(ltp) else ltp, 'oi': None if pd.isna(oi) else oi}
    return pd.Series({'snap': result})


def dump_snap_data(file_path):
    # Requires file with timestamp(tz), entity and OHLCV-Oi
    # ins_df, tokens, token_xref = get_req_contracts()

    # ','.join(ins_df['tradingsymbol'].tolist())

    df = pd.read_csv(file_path)
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S.%f')
    df['timestamp'] = pd.DatetimeIndex(df['timestamp']).tz_localize(IST)
    snap_df = df.groupby(['timestamp'], as_index=False).apply(ts_snap)
    print(snap_df)
    db_data = snap_df.to_dict('records')
    db_data = json.loads(json.dumps(db_data, default=str))
    DBHandler.insert_snap_data(db_data)


def calculate_oi_analytics(start, end):
    start = datetime.fromisoformat(start)
    end = datetime.fromisoformat(end)

    snap_dt = pd.date_range(start, end, freq='1min').to_pydatetime()

    ins_df, tokens, token_xref = get_req_contracts()
    shared_xref = {}
    # ana = SnapAnalysis(ins_df, tokens, token_xref, shared_xref, enable_scheduler=False)
    ana = SnapAnalysis(ins_df, tokens, token_xref, shared_xref, use_forward_fut=True, rate=0,
                       insert=True, enable_scheduler=False)
    # ana = SnapAnalysis(ins_df, tokens, token_xref, shared_xref, rate=10, insert=False, enable_scheduler=False)
    # ana = SnapAnalysis(ins_df, tokens, token_xref, shared_xref, rate=0, use_synthetic=True, insert=False, enable_scheduler=False)
    # ana = SnapAnalysis(ins_df, tokens, token_xref, shared_xref, rate=0, use_future=True, insert=False, enable_scheduler=False)
    snap_straddle = []
    for dt in snap_dt:
        logger.info(dt)
        data = DBHandler.get_snap_data(ts=dt)
        snap = data[0]['snap']
        greeks_df = ana.opt_calc(snap=snap, dt=dt)
        straddle_df = ana.straddle_calc(greeks_df)
        snap_straddle.append(straddle_df)

    result = pd.concat(snap_straddle, ignore_index=True, sort=False)
    result['timestamp'] = result['timestamp'].dt.tz_convert(IST).dt.tz_localize(None)
    # result.to_excel(f'vanilla_{start.strftime("%Y%m%d")}.xlsx', index=False)
    # result.to_excel(f'synthetic_{start.strftime("%Y%m%d")}.xlsx', index=False)
    # result.to_excel(f'futures_{start.strftime("%Y%m%d")}.xlsx', index=False)
    return result


if __name__ == '__main__':
    dump_snap_data(r'C:\Users\AP\DBeaverData\data_1min_202404041635.csv')
    calculate_oi_analytics(start='2024-04-04T09:15:00.000+0530', end='2024-04-04T15:29:00.000+0530')
