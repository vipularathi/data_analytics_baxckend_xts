import io
import os
from datetime import datetime
from functools import partial, reduce
from time import time

import pandas as pd
import requests
import csv

from common import logger, instruments_path, today, root_dir, data_dir, read_symbols
from update_expiry import update_expiry
from update_master import update_master

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

sym_df = pd.DataFrame()

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


def get_req_contracts():
    ent_exp = entity_expiry()
    scrips = ent_exp.keys()
    ins = get_instruments()
    nse_ins = ins[ins['exchange'].isin(['NSE', 'NFO'])].copy()
    eq_filter = nse_ins['tradingsymbol'].isin(scrips)
    der_filters = [eq_filter]
    for _name, _info in ent_exp.items():
        _der_filter = (nse_ins['name'].isin([_name])) & (nse_ins['expiry'].isin(_info['expiry']))
        der_filters.append(_der_filter)
    entity_filter = reduce(partial(lambda x, y: x | y), der_filters)
    req = nse_ins[entity_filter].copy()
    tokens = req['instrument_token'].tolist()
    token_xref = req[['instrument_token', 'tradingsymbol']].set_index('instrument_token').to_dict()['tradingsymbol']
    return req, tokens, token_xref


def entity_expiry():
    # symbols = pd.read_excel(os.path.join(root_dir, 'symbols.xlsx'))
    symbols = read_symbols
    #master file check
    master_path = os.path.join(data_dir, 'master.xlsx')
    if os.path.isfile(master_path):
        if datetime.fromtimestamp(os.path.getmtime(master_path)).date() < today.date():
            update_result = update_master()
            if update_result:
                logger.info(f'xts master file updated in the database')
            else:
                logger.error('Failed to update xts master file in the database')
    else:
        update_result = update_master()
        if update_result:
            logger.info(f'xts master file created and updated in the database')
        else:
            logger.error('Failed to create and update xts master file in the database')

    #symbol file(expiry) check
    sym_df = update_expiry()
    if not type(symbols['expiry'][0]) == type(pd.to_datetime(symbols['expiry'][0])):
        symbols['expiry'] = pd.to_datetime(symbols['expiry'], dayfirst=True)
    if not symbols[symbols['expiry'] < today].empty:
        sym_df = update_expiry()
        if sym_df:
            logger.info('\n expiry file updated')
        else:
            raise ValueError('Expired contracts filled')

    req = sym_df.groupby(['symbol']).agg({'expiry': set})
    req['expiry'] = req['expiry'].apply(list)
    return req.to_dict('index')