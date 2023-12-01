import io
import os
from datetime import datetime
from time import time

import pandas as pd
import requests

from common import logger, instruments_path, today

renames = {'NIFTY 50': 'NIFTY', 'NIFTY BANK': 'BANKNIFTY', 'NIFTY IT': 'NIFTYIT',
           'NIFTY FINANCIAL SERVICES': 'FINNIFTY', 'NIFTY FIN SERVICE': 'FINNIFTY',
           'NIFTY MID SELECT': 'MIDCPNIFTY', 'NIFTY MIDCAP SELECT': 'MIDCPNIFTY'}
header_kite_contract = {
    'accept': 'application/json, text/plain, */*',
    'origin': 'https://kite.zerodha.com',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36',
    'sec-fetch-site': 'same-site',
    'sec-fetch-mode': 'cors',
    'accept-encoding': 'gzip, deflate',
    'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7',

}


def get_raw_contracts():
    st = time()
    logger.debug("Downloading Zerodha contracts")
    headers = header_kite_contract

    contracts_url = 'http://api.kite.trade/instruments'
    response = requests.get(contracts_url, headers=headers)
    data = io.BytesIO(response.content)
    df = pd.read_csv(data)
    # ticker_rename = {'NIFTY 50': 'NIFTY', 'NIFTY BANK': 'BANKNIFTY', 'NIFTY IT': 'NIFTYIT'}
    # for org, rename in ticker_rename.items():
    for org, rename in renames.items():
        df.loc[df['tradingsymbol'] == org, 'tradingsymbol'] = rename
    logger.debug(f"Time taken for Zerodha Contracts: {time() - st} secs")
    return df


def get_instruments(force=False):
    is_valid_existing = False
    instruments_df = None

    if os.path.exists(instruments_path):
        logger.debug('Read existing contracts...')
        instruments_df = pd.read_csv(instruments_path, parse_dates=['expiry', 'lastUpdated'])
        valid_len = len(instruments_df[instruments_df['lastUpdated'] >= today])
        is_valid_existing = True if 0 < valid_len == len(instruments_df) else False

    if not is_valid_existing or force:
        logger.debug('Fetching contracts...')
        instruments = get_raw_contracts()
        instruments_df = pd.DataFrame(instruments)
        # instruments_df['expiry'] = pd.DatetimeIndex(instruments_df['expiry'])
        instruments_df['expiry'] = instruments_df['expiry'].apply(lambda x: None if pd.isna(x) else datetime.strptime(x, '%Y-%m-%d'))
        # instruments_df[(instruments_df['instrument_type'] == 'EQ') & (instruments_df['exchange'] == 'NSE')]  # NOSONAR
        instruments_df['lastUpdated'] = datetime.now()
        instruments_df.to_csv(instruments_path, index=False)
    return instruments_df
