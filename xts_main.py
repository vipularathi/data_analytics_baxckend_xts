import traceback
from threading import Thread
from concurrent.futures import ProcessPoolExecutor
import logging
import os
from time import sleep

import numpy as np
import requests
import pandas as pd
import socketio
import json
import multiprocessing as mp
# from multiprocessing import Manager
from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta

import db_ops
from common import today, logger
from data_handler import DataHandler, init_candle_creator

# import xts_models

# logger = logging.getLogger()
# logger.setLevel(logging.INFO)

host = "https://algozy.rathi.com:3000"
socket_url = "wss://algozy.rathi.com:3000"
subscription_url = f'{host}/apimarketdata/instruments/subscription'


# print entire dataframe
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

# epoch to datetime minus 5h 30m (GMT to IST)
epoch_to_datetime = lambda epoch_time: (datetime.fromtimestamp(epoch_time) - timedelta(hours=5, minutes=30)).time()


class QueueProcessing:
    def __init__(self, latest_feed_xref):
        self.queue = latest_feed_xref

    def start_processing(self):
        th = Thread(target=self.queue_processor)

        th.start()
        th.join()

    def queue_processor(self):
        q = {}
        while True:
            try:
                d = self.queue.get()

                event = d['event']
                d = json.loads(d['data'])
                if event == 1502:
                    d['Touchline']['LastUpdateTime'] = str(epoch_to_datetime(d['Touchline']['LastUpdateTime']))
                    try:
                        # check if instrument ID exists then update details, if not, add details
                        q.setdefault(d['ExchangeInstrumentID'], {}).update({
                            "LastUpdateTime": d['Touchline']['LastUpdateTime'],
                            "LastTradedPrice": d['Touchline']['LastTradedPrice'],
                            "LastTradedQunatity": d['Touchline']['LastTradedQunatity'],
                            "TotalTradedQuantity": d['Touchline']['TotalTradedQuantity'],
                            "TotalValueTraded": d['Touchline']['TotalValueTraded']
                        })
                    except:
                        pass

                if event == 1510:
                    # check if instrument ID exists then update details, if not, add details
                    q.setdefault(d['ExchangeInstrumentID'], {}).update({
                        "OpenInterest": d['OpenInterest']
                    })

                # logger.warning(len(q))  # will not exceed number of symbols - 211
                # logger.warning(q)
                print(q)

            except Exception as e:
                print(f"error in While: {traceback.format_exc()}")


# generate header
def gen_headers(tokens: list):
    headers = {}
    for token in tokens:
        # generate headers
        headers.setdefault(token, {}).update({
            'Authorization': token, 'Content-Type': 'application/json'
        })

    return headers


# test token validity - subscribe to ACC temporarily - pending
def test_token(token):

    subscription_payload = {token: create_payload([22], [1])}   # ACC for testing
    headers = gen_headers([token])
    status_code, status = subs(subscription_payload[token], headers[token])
    if status_code == 200:
        unsubs(subscription_payload[token], headers[token])
    else:
        return status_code

    return status_code


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
        print("choice is subs")
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
                print(status_list[i][1]["description"], status_list[i][1]["result"])
            else:
                print(f"Token {i+1} error message: ", status_list[i][1])    # unknown error handling
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


# push market data to queue
def push_data(token: str, userid: str, latest_feed_xref: dict, dict1: dict):
    entity_name = None

    try:
        # check live logs by passing params -> engineio_logger=True, logger=True
        socket = socketio.Client()

        @socket.on('connect')
        def on_connect():
            print('Connected to Socket')

        @socket.on('disconnect')
        def on_disconnect():
            print('Disconnected from Socket')
            exit()

        @socket.on('1502-json-full')    # market data
        def on_message_md(data):
            data = json.loads(data)

            if data['ExchangeInstrumentID'] in dict1.keys():
                entity_name = dict1[data['ExchangeInstrumentID']]     # take entity name from exchange id
                print(entity_name)
                print("data & entity: ", data['Touchline']['LastTradedPrice'], entity_name)
                ltp = data['Touchline']['LastTradedPrice']

                # latest_feed_xref.setdefault(entity_name, {}).update({
                #     "LastTradedPrice": ltp
                # })        # unprocessed - process into queue

                latest_feed_xref[entity_name] = {
                    "LastTradedPrice": ltp
                }

            print("Received test data: ", latest_feed_xref)     # testing

        @socket.on('1510-json-full')    # OI data
        def on_message_io(data):
            data = json.loads(data)

            if data['ExchangeInstrumentID'] in dict1.keys():
                entity_name = dict1[data['ExchangeInstrumentID']]     # take entity name from exchange id
                print(entity_name)
                print("IO data: ", data['OpenInterest'])
                oi = data['OpenInterest']

                # latest_feed_xref.setdefault(entity_name, {}).update({
                #     "OpenInterest": data['OpenInterest']
                # })        # unprocessed - process into queue

                latest_feed_xref[entity_name] = {
                    "OpenInterest": oi
                }

        # @socket.on('1105-json-partial')     # test static data
        # def on_message_test(data):
        #     print("Received test data", data)

        socket.connect(
            f"{socket_url}/apimarketdata/socket.io/?token={token}&userID={userid}&broadcastMode=Full&publishFormat=JSON",
            transports='websocket', socketio_path='apimarketdata/socket.io')

        socket.wait()
    except Exception as e:
        print(e)


def process_queue(latest_feed_xref):
    try:
        obj = QueueProcessing(latest_feed_xref)
        obj.start_processing()
    except Exception as e:
        print(f"error: {e}")


def get_token_header():

    # get creds directly from db
    creds = {'user1': {'appkey': '880cf50aaa5e4d495c1405', 'secretkey': 'Tjdk062@i1', 'userid': 'AA143'}, 'user2': {'appkey': '5c7e23fd3fff9c9cdb9126', 'secretkey': 'Aajs181$wG', 'userid': 'AA143'}}
    tokens = []
    userids = []

    for user, cred in creds.items():    # read directly from DB

        # check for data in DB - found
        if db_ops.select_creds(cred['appkey']) != 0:

            # get token from DB
            token = db_ops.select_creds(cred['appkey'])

            if test_token(token) == 400:

                # create new token & update in the DB - no need to test the new token
                token = create_token(cred['secretkey'], cred['appkey'])
                db_ops.update_creds(cred['appkey'], token)

            else:
                print("token test successful")

            # create combine dict - pending
            tokens.append(token)
            userids.append(cred['userid'])

        # data not found
        else:
            # generate token and insert in the DB
            token = create_token(cred['secretkey'], cred['appkey'])
            db_ops.insert_creds(cred['appkey'], cred['secretkey'], cred['userid'], token)

        tokens.append(token)

    headers = gen_headers(tokens)
    # choice = input("Subscribe / Unsubscribe (subs/unsubs): ").lower()
    choice = 'subs'     # static input - subscribe

    return tokens, headers, userids, choice


# we process the common dict here
def processing_data(access_tokens, userids, ch, latest_feed_xref):

    if ch == 'subs':
        with ProcessPoolExecutor(max_workers=4) as p1:
            # context = mp.Manager()
            # queue = context.Queue()

            for i in range(len(access_tokens)):
                p1.submit(push_data, access_tokens[i], userids[i], latest_feed_xref)  # push data in queue for each token
            # p1.submit(process_queue, latest_feed_xref, min_const, ltp, vol)         # process queue


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

        self.xts_ws = socketio.Client()
        self.xts_ws.on('connect', self.on_connect)
        self.xts_ws.on('disconnect', self.on_disconnect)
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

    def on_connect(self):
        print('Connected to Socket')

    def on_disconnect(self):
        print('Disconnected from Socket')

    # @socket.on('1502-json-full')  # market data
    def on_message_md(self, raw_data):
        data: dict = json.loads(raw_data)
        # logger.info(data)

        entity_name = self.xts_token_xref.get(data['ExchangeInstrumentID'], None)  # take entity name via exchange id
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


if __name__ == '__main__':
    pass

