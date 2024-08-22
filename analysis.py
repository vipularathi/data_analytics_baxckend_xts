import json
import traceback
from datetime import datetime
from time import sleep

import numpy as np
import pandas as pd
import py_vollib.black_scholes_merton.implied_volatility as bcm_iv
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from colorama import Style, Fore
from dateutil.relativedelta import relativedelta

from common import logger, today, holidays
from contracts import get_req_contracts
from db_ops import DBHandler
from greeks import get_greeks_intraday


def network_days(start: str, end: str, holidays_arr: list):
    b_days = pd.bdate_range(start=start, end=end, freq='C', weekmask='1111100', holidays=holidays_arr)
    b_days = b_days.tolist()
    b_days = [day + relativedelta(hour=15, minute=30, second=0, microsecond=0) for day in b_days]
    return len(b_days)


def diff_dt_num(dt1, dt2):
    hourly_diff = (dt1 - dt2).total_seconds() / 3600    # hourly diff - total seconds in dt / 3600 seconds in 1 hour
    time_diff_num = hourly_diff / 24         # day difference - hourly diff / 24 hrs in a day
    return time_diff_num                    # return numeric diff


def calc_dte(date_range):

    dte = [0 for _ in date_range]
    dates = []
    for i in range(len(date_range)):
        if i == 0:
            dte[i] = 0.0
        elif i == 1:
            dte[i] = max(diff_dt_num(dt1=date_range[i], dt2=date_range[i-1]) / 6.25 * 24, 0)
        else:
            dte[i] = max(network_days(start=date_range[1], end=date_range[i], holidays_arr=holidays) - 1 + dte[1],
                         network_days(start=date_range[1], end=date_range[i], holidays_arr=holidays) - 1)
        dates.append(date_range[i])

    dte = pd.DataFrame({'date': dates, 'dte': dte})
    return dte


def get_dte(row: pd.Series, dt):
    current_dt = [dt, dt.date() + relativedelta(hour=15, minute=30, second=0, microsecond=0)]  # static
    after_current_dt = pd.date_range(start=dt.date(), end=row['expiry'].date()).tolist()
    after_current_dt = [(date + relativedelta(hour=15, minute=30, second=0, microsecond=0)).to_pydatetime() for date in
                        after_current_dt]
    date_range = pd.Series(current_dt + after_current_dt)
    dte_all = calc_dte(date_range=date_range)
    dte = dte_all['dte'].iloc[-1]
    return dte


def get_greeks_dte(spot, strike, premium, dte, opt, r=0, q=0):
    t = dte / 365
    flag = str(opt)[:1].lower()
    try:
        iv = bcm_iv.implied_volatility(premium, spot, strike, t, r, q, flag) * 100
    except (bcm_iv.PriceIsBelowIntrinsic, bcm_iv.PriceIsAboveMaximum, Exception):
        iv = None
    return iv


class SnapAnalysis:

    def __init__(self, ins_df, tokens, token_xref, shared_xref, **kwargs):
        super().__init__()
        self.ins_df = ins_df
        self.tokens = tokens
        self.token_xref = token_xref
        self.shared_xref = shared_xref
        self.rate = kwargs.get('rate', 10)
        self.insert = kwargs.get('insert', True)
        self.enable_scheduler = kwargs.get('enable_scheduler', True)
        self.use_synthetic = kwargs.get('use_synthetic', False)
        self.use_forward_fut = kwargs.get('use_forward_fut', False)
        self.use_future = kwargs.get('use_future', False)

        self.opt_df = None
        self.fut_map = None
        self.prepare_meta()

        self.scheduler = None
        if self.enable_scheduler:
            self.scheduler: BackgroundScheduler = self.init_scheduler()
            self.scheduler.start()  # Scheduler starts

    def prepare_meta(self):
        req_df, token, token_xref = get_req_contracts()
        meta_df: pd.DataFrame = req_df[req_df['segment'] == 'NFO-OPT'].copy()
        renames = {'tradingsymbol': 'symbol', 'name': 'underlying', 'instrument_type': 'opt'}
        opt_df = meta_df[['tradingsymbol', 'name', 'expiry', 'strike', 'instrument_type']].copy().rename(columns=renames)
        opt_df['expiry'] = opt_df['expiry'].apply(lambda x: x + relativedelta(hour=15, minute=30))
        self.opt_df = opt_df
        # Check if FUT available
        fut_df: pd.DataFrame = req_df[req_df['segment'] == 'NFO-FUT'].copy()
        req_fut = fut_df.sort_values('expiry')[['tradingsymbol', 'name']].drop_duplicates(keep='first').rename(columns=renames, errors='ignore')
        self.fut_map = req_fut.set_index('underlying').to_dict()['symbol']

    def init_scheduler(self):
        if self.scheduler is not None:
            return self.scheduler

        executors = {
            'default': {'type': 'threadpool', 'max_workers': 5}
        }

        job_defaults = {
            'coalesce': True,
            'max_instances': 1,
            'misfire_grace_time': 10
        }
        self.scheduler = BackgroundScheduler(executors=executors, job_defaults=job_defaults, timezone='Asia/Kolkata',
                                             logger=logger)
        self.add_jobs_to_scheduler()
        return self.scheduler

    def add_jobs_to_scheduler(self):
        mkt_open = today + relativedelta(hour=9, minute=15)
        mkt_close = today + relativedelta(hour=15, minute=30)

        # Add jobs
        trigger_pre = IntervalTrigger(minutes=1, start_date=mkt_open - relativedelta(minutes=5), end_date=mkt_open)
        trigger_snap = IntervalTrigger(minutes=1, start_date=mkt_open, end_date=mkt_close)
        self.scheduler.add_job(self.run_analysis, trigger_pre, kwargs=dict(calc=False), id='snap_1',
                               name=f'SnapDataAnalysisPre')
        self.scheduler.add_job(self.run_analysis, trigger_snap, kwargs=dict(calc=True), id='snap_2',
                               name=f'SnapDataAnalysis')

    def run_analysis(self, calc=True):
        dt = datetime.now(tz=pytz.timezone('Asia/Kolkata')).replace(microsecond=0)
        xref = self.shared_xref.copy()
        logger.info(f'length of xref is {len(xref)}')
        data = {'timestamp': dt.isoformat(), 'snap': xref}
        db_data = json.loads(json.dumps(data, default=str))
        if self.insert:
            DBHandler.insert_snap_data([db_data])

        if calc:
            logger.info(f'calc values for {dt}')
            greeks_df = self.opt_calc(snap=xref, dt=dt)
            self.straddle_calc(greeks_df)

    @staticmethod
    def floor_strike(row):
        strikes = np.array(row['strike'])
        st = strikes[strikes <= row['current']].max()
        return st

    @staticmethod
    def mround_strike(row, price_col='current'):
        """Similar to excel mround function"""
        price = row[price_col]
        strikes = pd.Series(row['strike'])
        strikes = strikes.drop_duplicates().sort_values()
        base = (strikes[1:] - strikes.shift(1)[:-1]).min()
        m1 = base * int(price / base)
        m2 = m1 + base
        m = m1 if abs(price - m1) < abs(price - m2) else m2
        return m

    def opt_calc(self, snap, dt):
        opt_df: pd.DataFrame = self.opt_df.copy()
        opt_df['ltp'] = opt_df['symbol'].apply(self.get_ltp, args=(snap,))
        opt_df['oi'] = opt_df['symbol'].apply(self.get_oi, args=(snap,))
        if self.use_future:
            opt_df['spot'] = opt_df['underlying'].apply(lambda x: self.get_ltp(self.fut_map.get(x, x), snap))
        elif self.use_synthetic:
            underlyings = opt_df.groupby(['underlying', 'expiry'], as_index=False).agg({'strike': list})
            underlyings['current'] = underlyings['underlying'].apply(self.get_ltp, args=(snap,))
            underlyings['strike'] = underlyings.apply(self.floor_strike, axis=1)

            req_strikes = opt_df.merge(underlyings, how='inner', on=['underlying', 'expiry', 'strike'])
            req_strikes = req_strikes.drop(columns=['oi', 'current'])
            call_strikes = req_strikes[req_strikes['opt'] == 'CE'].drop(columns=['opt'])
            put_strikes = req_strikes[req_strikes['opt'] == 'PE'].drop(columns=['opt'])
            strike_df = call_strikes.merge(put_strikes, how='outer', on=['underlying', 'expiry', 'strike'], suffixes=('_call', '_put'))
            underlying_df = underlyings.merge(strike_df, how='left', on=['underlying', 'expiry', 'strike'])
            underlying_df['spot'] = underlying_df['ltp_call'].fillna(0) - underlying_df['ltp_put'].fillna(0) + underlying_df['strike']
            underlying_df = underlying_df[['underlying', 'expiry', 'spot']]

            opt_df = opt_df.merge(underlying_df, how='inner', on=['underlying', 'expiry'])
        elif self.use_forward_fut:
            underlyings = opt_df.groupby(['underlying', 'expiry'], as_index=False).agg({'strike': list})
            underlyings['current'] = underlyings['underlying'].apply(lambda x: self.get_ltp(self.fut_map.get(x, x), snap))
            underlyings.dropna(subset=['current'], inplace=True)

            logger.info(f"\nBefore apply: {underlyings.shape}")
            # logger.info(f"\nBefore apply strike is: \n{underlyings['strike']}")
            underlyings['strike'] = underlyings.apply(self.mround_strike, axis=1)
            logger.info(f"After apply: {underlyings.shape}")
            # logger.info(f"\nAfter apply strike is: \n{underlyings['strike']}")

            req_strikes = opt_df.merge(underlyings, how='inner', on=['underlying', 'expiry', 'strike'])
            req_strikes = req_strikes.drop(columns=['oi', 'current'])
            call_strikes = req_strikes[req_strikes['opt'] == 'CE'].drop(columns=['opt'])
            put_strikes = req_strikes[req_strikes['opt'] == 'PE'].drop(columns=['opt'])
            strike_df = call_strikes.merge(put_strikes, how='outer', on=['underlying', 'expiry', 'strike'], suffixes=('_call', '_put'))
            underlying_df = underlyings.merge(strike_df, how='left', on=['underlying', 'expiry', 'strike'])
            underlying_df['spot'] = underlying_df['ltp_call'].fillna(0) - underlying_df['ltp_put'].fillna(0) + underlying_df['strike']
            underlying_df = underlying_df[['underlying', 'expiry', 'spot']]

            opt_df = opt_df.merge(underlying_df, how='inner', on=['underlying', 'expiry'])
        else:
            opt_df['spot'] = opt_df['underlying'].apply(self.get_ltp, args=(snap,))

        greeks = opt_df.apply(
            lambda x: pd.Series(get_greeks_intraday(x['spot'], x['strike'], x['expiry'], x['opt'], x['ltp'], dt,
                                                    risk_free_rate=self.rate / 100)),
            axis=1)
        # Recalculate IV
        exp_df = opt_df.groupby(['underlying', 'expiry']).count()['strike'].reset_index().drop(columns=['strike'])
        exp_df['dte'] = exp_df.apply(get_dte, axis=1, dt=dt.replace(tzinfo=None))
        opt_df = opt_df.merge(exp_df, on=['underlying', 'expiry'])
        dte_iv = opt_df.apply(lambda x: get_greeks_dte(x['spot'], x['strike'], x['ltp'], x['dte'], x['opt']), axis=1)
        greeks['iv'] = dte_iv

        df = opt_df.join(greeks)
        df.insert(0, 'timestamp', dt)
        if self.insert:
            DBHandler.insert_opt_greeks(df.replace({np.NAN: None}).to_dict('records'))
        return df

    def straddle_calc(self, df: pd.DataFrame):
        call_df = df[(df['opt'] == 'CE')].copy()
        put_df = df[(df['opt'] == 'PE')].copy()

        # straddle values
        oc_df = call_df.merge(put_df, on=['timestamp', 'underlying', 'expiry', 'strike'], how='outer', suffixes=('_c', '_p'))
        oc_df.sort_values(['timestamp', 'underlying', 'expiry', 'strike'], inplace=True)
        non_zero = (oc_df['ltp_c'] != 0) & (oc_df['ltp_p'] != 0)
        oc_df.loc[non_zero, 'combined_premium'] = (oc_df['ltp_c'] + oc_df['ltp_p'])[non_zero]
        oc_df.loc[non_zero, 'combined_iv'] = (oc_df[['iv_c', 'iv_p']].mean(axis=1))[non_zero]
        # ATM calc
        # call_otm = oc_df['strike'] >= oc_df['spot_c']
        atm_df = oc_df.groupby(['underlying', 'expiry', 'spot_c'], as_index=False).agg({'strike': list})

        logger.info(f"\nBefore apply: {atm_df.shape}")
        logger.info(f"\nBefore apply implied atm is: \n{atm_df['implied_atm']}")
        atm_df['implied_atm'] = atm_df.apply(self.mround_strike, axis=1, price_col='spot_c')
        logger.info(f"\nAfter apply: {atm_df.shape}")
        logger.info(f"\nAfter apply implied atm is: \n{atm_df['implied_atm']}")
        
        atm_df = atm_df[['underlying', 'expiry', 'implied_atm']].copy()
        oc_df = oc_df.merge(atm_df, on=['underlying', 'expiry'])  # map atm
        call_otm = oc_df['strike'] > oc_df['implied_atm']
        # OTM IV
        oc_df['otm_iv'] = np.where(call_otm, oc_df['iv_c'].values, oc_df['iv_p'].values)
        min_combined = oc_df.groupby(['timestamp', 'underlying', 'expiry'], as_index=False).agg({'combined_premium': 'min'})
        min_combined['minima'] = True
        minima_df = oc_df.merge(min_combined, on=['timestamp', 'underlying', 'expiry', 'combined_premium'], how='left')
        minima_df['minima'].fillna(False, inplace=True)
        # oc_df['minima'] = oc_df['combined_premium'] == min_combined

        req_cols = ['timestamp', 'underlying', 'expiry', 'strike', 'symbol_c', 'symbol_p', 'spot_c', 'ltp_c', 'ltp_p',
                    'oi_c', 'oi_p', 'iv_c', 'iv_p', 'combined_premium', 'combined_iv', 'otm_iv', 'minima']
        req_straddle_df = minima_df[req_cols].copy()
        cols_renames = {'symbol_c': 'call', 'symbol_p': 'put', 'spot_c': 'spot', 'ltp_c': 'call_price',
                        'ltp_p': 'put_price', 'oi_c': 'call_oi', 'oi_p': 'put_oi', 'iv_c': 'call_iv', 'iv_p': 'put_iv'}
        req_straddle_df.rename(columns=cols_renames, inplace=True)
        if self.insert:
            # insert req_straddle_df
            DBHandler.insert_opt_straddle(req_straddle_df.replace({np.NAN: None}).to_dict('records'))
        return req_straddle_df

    @staticmethod
    def get_ltp(symbol: str, xref: dict):
        return xref.get(symbol, {}).get('last_price', None)

    @staticmethod
    def get_oi(symbol: str, xref: dict):
        return xref.get(symbol, {}).get('oi', None)


def start_analysis(ins_df, tokens, token_xref, shared_xref):
    try:
        SnapAnalysis(ins_df, tokens, token_xref, shared_xref, use_forward_fut=True, rate=0)

        while True:
            sleep(10)
    except Exception as exc:
        logger.error(f'{Style.BRIGHT}{Fore.GREEN}Error in initiating Strategy Data Processor: {exc}\n{"*" * 15} Requires immediate attention {"*" * 15}{Style.RESET_ALL}')
        logger.error(traceback.format_exc())
