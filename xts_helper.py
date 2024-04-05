import os
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import requests
from common import today, logger

epoch_to_datetime = lambda epoch_time: datetime.fromtimestamp(epoch_time)

NSECM_head_str = "ExchangeSegment|ExchangeInstrumentID|InstrumentType|Name|Description|Series|NameWithSeries|InstrumentID|PriceBand.High|PriceBand.Low|FreezeQty|TickSize|LotSize|Multiplier|DisplayName|ISIN|PriceNumerator|pricedenominotor|FullDescription"
NSEFO_fut_head_str = "ExchangeSegment|ExchangeInstrumentID|InstrumentType|Name|Description|Series|NameWithSeries|InstrumentID|PriceBand.High|PriceBand.Low|FreezeQty|TickSize|LotSize|Multiplier|UnderlyingInstrumentId|UnderlyingIndexName|ContractExpiration|DisplayName|PriceNumerator|pricedenominotor|FullDescription"
NSEFO_opt_head_str = "ExchangeSegment|ExchangeInstrumentID|InstrumentType|Name|Description|Series|NameWithSeries|InstrumentID|PriceBand.High|PriceBand.Low|FreezeQty|TickSize|LotSize|Multiplier|UnderlyingInstrumentId|UnderlyingIndexName|ContractExpiration|StrikePrice|OptionType|DisplayName|PriceNumerator|pricedenominotor|FullDescription"


def contracts_master(payload):
    head = {"Content-Type": "application/json"}

    response = requests.post(
        "https://algozy.rathi.com:3000/apimarketdata/instruments/master",
        headers=head,
        json=payload
    )

    data = response.json()

    if not os.path.exists('entity_data'):
        os.makedirs('entity_data')

    # csv for each exchange
    for exchange in payload["exchangeSegmentList"]:
        if exchange == 'NSECM':
            with open(f"entity_data/{exchange}.csv", "w") as file:
                file.write((data['result']).replace("|", ","))
        elif exchange == 'NSEFO':
            with open(f"entity_data/{exchange}.csv", "w") as file:
                file.write((data['result']).replace("|", ","))


def csv_operation(payload: dict, exchanges: list, force: bool):
    for i in exchanges:
        payload['exchangeSegmentList'] = [i]
        contracts_master(payload)

    NSEFO_fut_head = NSEFO_fut_head_str.split("|")  # list of column headers
    NSEFO_opt_head = NSEFO_opt_head_str.split("|")  # list of column headers
    NSECM_head = NSECM_head_str.split("|")  # list of column headers

    df1 = pd.read_csv('entity_data/NSECM.csv')
    df1.columns = NSECM_head + ["unknown-1", "unknown-2", "unknown-3"]     # 3 unknown columns
    df2 = pd.read_csv('entity_data/NSEFO.csv', names=range(23))

    fut_list = ["FUTSTK", "FUTIDX"]
    opt_list = ["OPTSTK", "OPTIDX"]

    df2_fut = df2[df2[5].isin(fut_list)]
    df2_opt = df2[df2[5].isin(opt_list)]

    # FO - future
    futures = df2_fut[list(range(21))]
    futures.columns = NSEFO_fut_head

    # FO - options
    options = df2_opt
    options.columns = NSEFO_opt_head

    # FO-fut + FO-opt + CM
    df_fo = pd.concat([df1, futures, options], ignore_index=True, sort=False)

    # remove temp files
    os.remove("entity_data/NSECM.csv")
    os.remove("entity_data/NSEFO.csv")

    # check csv exists
    if os.path.exists("entity_data/Instruments_xts.csv"):
        modified_time = epoch_to_datetime(os.path.getmtime("entity_data/Instruments_xts.csv"))
        if modified_time.date() == (datetime.now()).date():
            logger.info("csv already created today!!")
            if force:
                df_fo.to_csv("entity_data/Instruments_xts.csv", index=False)
                logger.info("csv created!!")
    else:
        logger.info("csv already created today!!")
        df_fo.to_csv("entity_data/Instruments_xts.csv", index=False)
        logger.info("csv already created today!!")


if __name__ == '__main__':

    payload = {"exchangeSegmentList": []}
    exchanges = ['NSECM', 'NSEFO']

    force = False     # do not force create csv
    csv_operation(payload, exchanges, force)
