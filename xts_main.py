import traceback
from threading import Thread
from concurrent.futures import ProcessPoolExecutor
from time import sleep

import jwt
import numpy as np
import requests
import pandas as pd
import socketio
import json
import multiprocessing as mp
# from multiprocessing import Manager
from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta

from db_ops import DBHandler
from common import today, logger
from data_handler import DataHandler, init_candle_creator


host = "https://algozy.rathi.com:3000"
socket_url = "wss://algozy.rathi.com:3000"
subscription_url = f'{host}/apimarketdata/instruments/subscription'


# generate header
def gen_headers(tokens: list):
    headers = {}
    for token in tokens:
        # generate headers
        headers.setdefault(token, {}).update({
            'Authorization': token, 'Content-Type': 'application/json'
        })

    return headers


# test token validity - check expiry date
def test_token(token):
    head = {"Authorization": token}
    response = requests.get(f"{host}/apimarketdata/config/clientConfig", headers=head)

    if response.status_code == 200:
        logger.info("Token Not Expired")
    else:
        logger.info("Token Expired")

    return response.status_code


def split_into_tokens(tokens: list, df):

    n = len(tokens)

    # splitting to n equal parts
    temp = np.array_split(df, n)

    return temp


# payload + subscribe + status
def subscribe_init(tokens, headers, ch, df):
    # create payload - for each token
    subscription_payload = {}
    subscription_payload_oi = {}
    for i in range(len(tokens)):
        inst_id, exch_seg = list(df[i]['exchange_token']), list(df[i]['exchange'])
        subscription_payload[tokens[i]] = create_payload(inst_id, exch_seg)  # market data
        subscription_payload_oi[tokens[i]] = create_payload_oi(inst_id, exch_seg)  # open interest

    status_list = []

    if ch == 'subs':
        # subscribe symbols - for each token
        for token in tokens:
            status_code, status = subs(subscription_payload[token], headers[token])     # market data
            status_list.append([status_code, status])
            status_code, status = subs(subscription_payload_oi[token], headers[token])  # open interest
            status_list.append([status_code, status])

    elif ch == 'unsubs':
        # unsubscribe symbols - for each token
        for token in tokens:
            status_code, status = unsubs(subscription_payload[token], headers[token])   # market data
            status_list.append([status_code, status])
            status_code, status = unsubs(subscription_payload_oi[token], headers[token])    # open interest
            status_list.append([status_code, status])

    # check if all token subscription status are 200
    flag = 0
    for i in range(len(status_list)):
        if status_list[i][0] != 200:
            status_list[i][1] = json.loads(status_list[i][1])
            if status_list[i][1]["code"] == "e-session-0002":     # already subscribed
                logger.warn(f'{status_list[i][1]["description"]}, {status_list[i][1]["result"]}')
            else:
                logger.warn(f"Token {i+1} error message: ", status_list[i][1])  # unknown error handling
                flag += 1

    if flag > 0:
        return "Unsuccessful"       # response status not 200 due to unknown error
    else:
        return "Successful"


# create payload - Market data
def create_payload(inst_id: list = None, exch_seg: list = None):

    subscription_payload = {
        "instruments": [],
        "xtsMessageCode": 1502   # market data (1502)
    }

    for i in range(len(inst_id)):
        if exch_seg[i] == 'NSE':
            exch_seg[i] = 1
        elif exch_seg[i] == 'NFO':
            exch_seg[i] = 2

        data = {"exchangeSegment": exch_seg[i], "exchangeInstrumentID": inst_id[i]}
        subscription_payload["instruments"].append(data)

    return subscription_payload


# create payload - OI
def create_payload_oi(inst_id: list = None, exch_seg: list = None):

    subscription_payload = {
        "instruments": [],
        "xtsMessageCode": 1510   # Open Interest (1510)
    }
    for i in range(len(inst_id)):
        data = {"exchangeSegment": exch_seg[i], "exchangeInstrumentID": inst_id[i]}
        subscription_payload['instruments'].append(data)

    return subscription_payload


# create token
def create_token(secretkey: str = "Tjdk062@i1", appkey: str = "880cf50aaa5e4d495c1405"):
    url = f"{host}/apimarketdata/auth/login"
    headers = {
            "secretKey": secretkey,
            "appKey": appkey,
            "source": "WebAPI",
    }
    response = requests.post(url, json=headers)
    response = response.json()
    try:
        return response["result"]["token"]
    except:
        return "Token not generated"


# subscription request
def subs(subscription_payload, headers):
    subscription_payload = json.dumps(subscription_payload, default=str)
    subscription_payload = json.loads(subscription_payload)
    subscription_response = requests.post(subscription_url, headers=headers, json=subscription_payload)
    return subscription_response.status_code, subscription_response.text


# unsubscribe request
def unsubs(subscription_payload, headers):
    subscription_payload = json.dumps(subscription_payload, default=str)
    subscription_payload = json.loads(subscription_payload)
    subscription_response = requests.put(subscription_url, headers=headers, json=subscription_payload)
    return subscription_response.status_code, subscription_response.text


def get_token_header():

    creds = {}
    tokens = []
    userids = []

    # get credentials from DB
    res = DBHandler.get_credentials()
    if len(res) == 0:
        raise RuntimeError('No XTS credentials available for connection')
    for i in range(len(res)):

        # add to dict
        creds.setdefault(f"user{i+1}", {}).update({
            "appkey": str(res['appkey'][i]),
            "secretkey": str(res['secretkey'][i]),
            "userid": str(res['userid'][i]),
            "token": str(res['token'][i])
        })

    # iterate through all credentials
    for user, cred in creds.items():
        token = cred['token']

        if test_token(token) == 400:

            # create new token & update in the DB - no need to test the new token
            token = create_token(cred['secretkey'], cred['appkey'])
            DBHandler.update_credentials(cred['appkey'], token)

        else:
            logger.info("token test successful")

        # create combine dict - pending
        tokens.append(token)
        userids.append(cred['userid'])

    headers = gen_headers(tokens)
    choice = 'subs'     # static input - subscribe

    return tokens, headers, userids, choice


class XtsWS:
    close_time = today + relativedelta(hour=15, minute=35)

    def __init__(self, tokens, token_xref, scrips, access_token, user_id, candle_send, start=False, name='1', **kwargs):
        super().__init__()
        self.tokens = tokens
        self.scrips = scrips
        self.token_xref = token_xref
        self.access_token = access_token
        self.user_id = user_id

        self.executor = None  # Common context
        self.handler = None  # Unique Every instance
        self.candle_fut = None  # Unique Every instance
        self.hist_flag = None  # Unique Every instance
        self.candle_send = candle_send  # Common channel
        self.latest_feed_xref = kwargs.get('latest_feed_xref', None)
        self.xts_token_xref = kwargs.get('xts_token_xref', {})

        self.xts_ws = socketio.Client(request_timeout=120, logger=False, engineio_logger=False, ssl_verify=False)
        self.xts_ws.on('connect', self.on_connect)
        self.xts_ws.on('disconnect', self.on_disconnect)
        self.xts_ws.on('error', self.on_error)
        self.xts_ws.on('1502-json-full', self.on_message_md)
        self.xts_ws.on('1510-json-full', self.on_message_io)
        # self.xts_ws.on('1105-json-partial', self.on_message_test)

        self.entity_oi_xref = {}
        self.name = f"soc_{name}"

        if start:
            self.start()

    def start(self):
        with ProcessPoolExecutor(max_workers=4) as executor:
            if self.executor is None:
                self.executor = executor

            manager = mp.Manager()
            self.hist_flag = manager.Event()
            self.hist_flag.set()
            # Pipes for data processing
            feed_receiver, feed_send = mp.Pipe(duplex=False)
            # Initialize Data Handler (Middleware between Broadcast and Candle)
            self.handler = DataHandler(sender=feed_send)
            self.handler.start_processor()

            # Initialize Candle Handler
            # noinspection PyTypeChecker
            self.candle_fut = self.executor.submit(init_candle_creator, self.scrips, self.tokens, self.token_xref,
                                                   feed_receiver, start=True, candle_sender=self.candle_send,
                                                   threaded=True, hist_flag=self.hist_flag, name=self.name,
                                                   shared_xref=self.latest_feed_xref, update_redis=True, mode='xts')

            ws_url = f"{socket_url}/apimarketdata/socket.io/?token={self.access_token}&userID={self.user_id}&broadcastMode=Full&publishFormat=JSON"
            self.xts_ws.connect(ws_url, transports='websocket', socketio_path='apimarketdata/socket.io')
            self.xts_ws.wait()

            while True:
                logger.info("Non-waiting state")
                sleep(30)

    def on_connect(self):
        logger.info("Connected to Socket")

    def on_disconnect(self):
        logger.info("Disconnected from Socket")

    def on_error(self, data):
        logger.info(f"Error from Socket: {data}")

    # @socket.on('1502-json-full')  # market data
    def on_message_md(self, raw_data):
        data: dict = json.loads(raw_data)
        # logger.info(data)

        entity_name = self.xts_token_xref.get(data['ExchangeInstrumentID'], None)  # get entity name via exchange id
        if entity_name:
            data.update({'entity': entity_name, 'oi': self.entity_oi_xref.get(entity_name, None)})
            ticks = [data]
            self.handler.receiver(ticks)

    # @socket.on('1510-json-full')  # OI data
    def on_message_io(self, raw_data):
        data = json.loads(raw_data)
        # logger.info(data)

        entity_name = self.xts_token_xref.get(data['ExchangeInstrumentID'], None) # take entity name via exchange id
        if entity_name:
            self.entity_oi_xref[entity_name] = data['OpenInterest']

    # @socket.on('1105-json-partial')     # test data - unsubscribed
    # def on_message_test(self, raw_data):
    #     print("Received test data", raw_data)


def xts_wrapper(*args, **kwargs):
    try:
        xts = XtsWS(*args, **kwargs)
        xts.start()
    except Exception as ec:
        logger.error(f'Error in XTS Wrapper: {ec}')
    logger.error("XTS wrapper exits")


if __name__ == '__main__':
    pass

