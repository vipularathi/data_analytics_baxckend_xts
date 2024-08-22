import os
import glob
import random
import requests
import pandas as pd
from datetime import datetime
import openpyxl
from tqdm import tqdm
from common import logger, root_dir, data_dir, logs_dir, host, access_token
# from xts_connect import login, host
from db_config import n_tbl_master, s_tbl_master
from db_ops import insert_data_df

# access_token = login()
master_path = os.path.join(data_dir,'master.xlsx')
raw_path = os.path.join(data_dir, 'raw_master.xlsx')

if os.path.isfile(master_path):
    # ran = random.randint(1,10)
    # master_path = os.path.join(root_dir, f'master_{ran}.xlsx')
    last_update_date = datetime.fromtimestamp(os.path.getmtime(master_path)).date()
    logger.info(f'master file was last updated on {last_update_date}')

# required_cols = ['ExchangeSegment', 'ExchangeInstrumentID', 'InstrumentType', 'Name', 'Description', 'Series', 'NameWithSeries', 'InstrumentID', 'PriceBand.High', 'PriceBand.Low', 'FreezeQty', 'TickSize', 'LotSize', 'Multiplier', 'UnderlyingInstrumentId', 'UnderlyingIndexName', 'ContractExpiration', 'StrikePrice', 'OptionType', 'DisplayName', 'PriceNumerator', 'pricedenominotor', 'Description']

def raw_master(exch_seg):
    url = f'{host}/apimarketdata/instruments/master'
    payload = {
        "exchangeSegmentList": exch_seg
    }
    headers = {
        "Content-Type": "application/json",
        "authorization": access_token
    }
    response = requests.post(url, json=payload, headers=headers)
    if response.status_code == 200:
        logger.info(f'Fetching the data for only {exch_seg}')
        logger.info(f'Fetching raw data into dataframe . . .')
        # result = response.json().get('result', '')
        # data = pd.DataFrame([row.split('|') for row in result.split('\n')])
        # data.to_excel(master_path)
        # print(f'Master file downloaded at {master_path}')
        # print(f'\ndata is \n {data.head()}')
        # return 'Master Downloaded Successfully!!!'
        final_response = pd.DataFrame(response.json()["result"].split("\n"))
        # final_response.to_csv('final_resp.csv', index=False)
        # fi = final_response[0]
        # fii = final_response[0].str
        final_response1 = final_response[0].str.split("|", expand=True) #since final_response has only 1 col
        # final_response1.to_excel(raw_path)
        return final_response1
    else:
        print(f"Error in fetching instrument master. Status code: {response.status_code}")
        return None

def download_master(exch_seg):
    df = raw_master(exch_seg)
    logger.info(f'Raw dataframe sent for trucation')
    # for cols in df.columns:
    #     if cols.lower().find("unnamed") != -1:
    #         del df[cols]
    # return df
    # df = pd.read_excel(raw_path)
    # df = df.drop('Unnamed: 0', axis=1)
    # df_fo = df[df[0] == 'NSEFO']
    # df_fo = df_fo[~df_fo[3].str.endswith('NSETEST')]
    # df_fo[16] = df_fo[16].apply(lambda row: pd.to_datetime(row).date())

    pbar = tqdm(total=100, desc='Processing master data', unit='%')
    pbar.update(20)
    df_fo = df[df[0] == 'NSEFO']
    df_fo = df_fo[~df_fo[3].str.endswith('NSETEST')]
    df_fo[16] = df_fo[16].apply(lambda row: pd.to_datetime(row).date())
    pbar.update(20)

    def set_opt_type(row):
        if str(row[4]).endswith('FUT'):
            return 'XX'
        elif str(row[4]).endswith('CE'):
            return 'CE'
        elif str(row[4]).endswith('PE'):
            return 'PE'
        else:
            return row[4]

    df_fo['opt_type'] = df_fo.apply(set_opt_type, axis=1)
    df_fo[17] = df_fo[17].apply(lambda row: row if row.isdigit() else '')
    df_fo.rename(columns={1: 'scripcode', 2: 'exchange', 3: 'symbol', 4: 'name', 5: 'opt', 16: 'expiry', 17: 'strike'},
                 inplace=True)
    # df_fo = df_fo.loc[:, ~df_fo.columns.str.isdigit()]
    df_fo = df_fo.loc[:, [col for col in df_fo.columns if not isinstance(col, int)]]
    df_fo.reset_index(drop=True, inplace=True)
    pbar.update(20)
    # update and close the progress bar
    # pbar.update(total_rows)

    df_fo.to_excel(master_path, sheet_name='NSEFO', index=False)
    pbar.update(20)
    if os.path.isfile(master_path):
        pbar.update(20)
        pbar.close()
        logger.info(f'Master table downloaded and updated at {master_path}')

    # with pd.ExcelWriter(master_path, engine='openpyxl') as writer: # remove if xlsx file is not needed
    #     df_fo.to_excel(writer, sheet_name=exch_seg, index=False)
    #     logger.info(f'Master table downloaded and updated at {master_path}')
    return df_fo

def update_master():
    master_df = download_master(['NSEFO'])
    insert_data_df(table=n_tbl_master, data=master_df, master=True)
    return True

# update_master_result = update_master()
# print(update_master_result)