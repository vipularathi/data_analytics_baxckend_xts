import os
from datetime import datetime, timedelta
from io import StringIO
import pandas as pd
import numpy as np
import requests
from common import today, logger

epoch_to_datetime = lambda epoch_time: datetime.fromtimestamp(epoch_time)

NSECM_head_str = "ExchangeSegment|ExchangeInstrumentID|InstrumentType|Name|Description|Series|NameWithSeries|InstrumentID|PriceBand.High|PriceBand.Low|FreezeQty|TickSize|LotSize|Multiplier|DisplayName|ISIN|PriceNumerator|pricedenominotor|FullDescription"
NSEFO_fut_head_str = "ExchangeSegment|ExchangeInstrumentID|InstrumentType|Name|Description|Series|NameWithSeries|InstrumentID|PriceBand.High|PriceBand.Low|FreezeQty|TickSize|LotSize|Multiplier|UnderlyingInstrumentId|UnderlyingIndexName|ContractExpiration|DisplayName|PriceNumerator|pricedenominotor|FullDescription"
NSEFO_opt_head_str = "ExchangeSegment|ExchangeInstrumentID|InstrumentType|Name|Description|Series|NameWithSeries|InstrumentID|PriceBand.High|PriceBand.Low|FreezeQty|TickSize|LotSize|Multiplier|UnderlyingInstrumentId|UnderlyingIndexName|ContractExpiration|StrikePrice|OptionType|DisplayName|PriceNumerator|pricedenominotor|FullDescription"


def contracts_df(payload):
    """
    get response and create dataframe
    """

    head = {"Content-Type": "application/json"}
    response = requests.post(
        "https://algozy.rathi.com:3000/apimarketdata/instruments/master",
        headers=head,
        json=payload
    )

    data = response.json()

    if not os.path.exists('entity_data'):
        os.makedirs('entity_data')

    df = None
    # csv for each exchange
    for exchange in payload["exchangeSegmentList"]:
        if exchange == 'NSECM':
            file1 = StringIO((data['result']).replace("|", ","))
            df = pd.read_csv(file1)
        elif exchange == 'NSEFO':
            file2 = StringIO((data['result']).replace("|", ","))
            df = pd.read_csv(file2, names=range(23), low_memory=False)  # low_memory: avoid warning in console

    return df


def create_csv(payload: dict, exchanges: list):
    """
    create payload, assign headers and generate csv from response.
    """
    df = {}
    for i in exchanges:
        payload['exchangeSegmentList'] = [i]
        df[i] = contracts_df(payload)

    NSEFO_fut_head = NSEFO_fut_head_str.split("|")  # list of column headers
    NSEFO_opt_head = NSEFO_opt_head_str.split("|")  # list of column headers
    NSECM_head = NSECM_head_str.split("|")  # list of column headers

    df1 = df['NSECM']
    df1.columns = NSECM_head + ["unknown-1", "unknown-2", "unknown-3"]  # 3 unknown columns
    df2 = df['NSEFO']

    fut_list = ["FUTSTK", "FUTIDX"]
    opt_list = ["OPTSTK", "OPTIDX"]

    # replace UnderlyingIndexName values
    di = {"Nifty 50": "NIFTY", "Nifty Fin Service": "FINNIFTY", "Nifty Bank": "BANKNIFTY", "NIFTY MID SELECT": "MIDCPNIFTY"}
    df2[15] = df2[15].apply(lambda x: di.get(x) if di.get(x, None) is not None else x)

    df2_fut = df2[df2[5].isin(fut_list)]
    df2_opt = df2[df2[5].isin(opt_list)]

    # FO - future
    futures = df2_fut[list(range(21))]
    futures.columns = NSEFO_fut_head

    # FO - options
    options = df2_opt
    options.columns = NSEFO_opt_head

    # CM + FO-fut + FO-opt
    df_fo = pd.concat([df1, futures, options], ignore_index=True, sort=False)
    df_fo.to_csv("entity_data/Instruments_xts.csv", index=False)
    logger.info("Instrument csv created")


def csv_operations(payload: dict, exchanges: list, force: bool):
    """
    check if csv generated for today. if not, generate.
    """
    date_mismatch = False
    exists = True

    # check csv exists
    if os.path.exists("entity_data/Instruments_xts.csv"):
        modified_time = epoch_to_datetime(os.path.getmtime("entity_data/Instruments_xts.csv"))
        if modified_time.date() == (datetime.now()).date():
            logger.info("Instrument csv already created today.")
            if not force:
                logger.info("csv generation aborting!!")
        else:
            logger.info("Instrument csv not created today.")
            date_mismatch = True
    else:
        logger.info("Instrument csv not found.")
        exists = False

    if force or date_mismatch or (not exists):
        create_csv(payload, exchanges)


if __name__ == '__main__':

    payload = {"exchangeSegmentList": []}
    exchanges = ['NSECM', 'NSEFO']

    force = False     # do not force create csv
    csv_operations(payload, exchanges, force)
