from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from multiprocessing import Manager, Pipe
from time import sleep

import requests
from dateutil.relativedelta import relativedelta
from kiteconnect import KiteTicker, KiteConnect

from common import today, logger
from data_handler import DataHandler, init_candle_creator

manual = False
api_key = '8a6f62gf3y0ei7o9'
token_url = 'http://chart.tradeclue.com/z_token'
z_token = requests.get(token_url).json()['ztoken']
api_access_token = z_token


def initiate_session(override=False):
    """
    Use to create session with Zerodha and login. If access token is not empty then it is used for session.
    :param override: bool
            Use this to forcefully create new token even access token is present already
    :return: KiteConnect object
    """
    if api_access_token is not None and api_access_token != '' and not override:
        logger.info(f'Using Access Token: {api_access_token}')
        kite = KiteConnect(api_key=api_key, access_token=api_access_token)
    else:
        raise ValueError('Missing values')

    return kite


# noinspection PyUnusedLocal
def on_ticks(ws, ticks):
    """
    -  Triggered when ticks are received.
        - `ticks` - List of `tick` object. Check below for sample structure.
    """
    logger.info(f"Ticks: {ticks}")


# noinspection PyUnusedLocal
def on_close(ws, code, reason):
    """
    -  Triggered when connection is closed.
        - `code` - WebSocket standard close event code (https://developer.mozilla.org/en-US/docs/Web/API/CloseEvent)
        - `reason` - DOMString indicating the reason the server closed the connection
    """
    logger.info(f'Closed: {code}: {reason}')
    ws.stop()  # Don't call stop if you want reconnect mechanism to kick in


# noinspection PyUnusedLocal
def on_error(ws, code, reason):
    """
    -  Triggered when connection is closed with an error.
        - `code` - WebSocket standard close event code (https://developer.mozilla.org/en-US/docs/Web/API/CloseEvent)
        - `reason` - DOMString indicating the reason the server closed the connection
    """
    logger.info(f'Error in Websocket. {code}: {reason}')


# noinspection PyUnusedLocal
def on_connect(ws, response):
    """
    -  Triggered when connection is established successfully.
        - `response` - Response received from server on successful connection.
    """
    logger.info(f"Websocket Connected: {response}")
    tokens = [260105, 256265]  # NIFTY and BANKNIFTY
    ws.set_mode(ws.MODE_LTP, tokens)


# noinspection PyUnusedLocal
def on_message(ws, payload, is_binary):
    """
    -  Triggered when message is received from the server.
        - `payload` - Raw response from the server (either text or binary).
        - `is_binary` - Bool to check if response is binary type.
    """
    # Triggered on every message and should be avoided to use.
    pass


# noinspection PyUnusedLocal
def on_reconnect(ws, attempts_count):
    """
    -  Triggered when auto reconnection is attempted.
    - `attempts_count` - Current reconnect attempt number.
    """
    logger.info(f'Trying to reconnect. Attempt: {attempts_count}')


# noinspection PyUnusedLocal
def on_no_reconnect(ws):
    """
    -  Triggered when number of auto reconnection attempts exceeds `reconnect_tries`.
    """
    logger.info("Couldn't reconnect")


# noinspection PyUnusedLocal
def on_order_update(ws, data):
    """
    -  Triggered when there is an order update for the connected user.
    """
    logger.info(f"Order Update: {data}")


def initiate_websocket(client):
    kws = KiteTicker(client.api_key, client.access_token)
    # Set default methods for each call
    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.on_close = on_close
    kws.on_error = on_error
    kws.on_reconnect = on_reconnect
    kws.on_message = on_message

    return kws


class ZerodhaWS:
    close_time = today + relativedelta(hour=15, minute=35)

    def __init__(self, tokens, token_xref, scrips, client, candle_send, start=False, name='1', **kwargs):
        super().__init__()
        self.tokens = tokens
        self.token_xref = token_xref
        self.scrips = scrips
        self.client = client  # Common copy for all instances
        self.executor = None  # Common context
        self.handler = None  # Unique Every instance
        self.candle_fut = None  # Unique Every instance
        self.hist_flag = None  # Unique Every instance
        self.candle_send = candle_send  # Common channel
        self.latest_feed_xref = kwargs.get('latest_feed_xref', None)

        self.kws = initiate_websocket(client)
        self.kws.on_connect = self.ws_on_connect
        self.kws.on_ticks = self.ws_on_ticks
        self.kws.on_close = self.ws_on_close
        self.kws.on_error = self.ws_on_error

        self.name = f"soc_{name}"

        if start:
            self.start()

    def start(self):
        with ProcessPoolExecutor(max_workers=4) as executor:
            if self.executor is None:
                self.executor = executor

            mp = Manager()
            self.hist_flag = mp.Event()
            self.hist_flag.set()
            # Pipes for data processing
            feed_receiver, feed_send = Pipe(duplex=False)
            # Initialize Data Handler (Middleware between Broadcast and Candle)
            # self.handler = DataHandler(sender=None)
            self.handler = DataHandler(sender=feed_send)
            self.handler.start_processor()

            # Initialize Candle Handler
            # noinspection PyTypeChecker
            self.candle_fut = self.executor.submit(init_candle_creator, self.scrips, self.tokens, self.token_xref,
                                                   feed_receiver, start=True, candle_sender=self.candle_send,
                                                   threaded=True, hist_flag=self.hist_flag, name=self.name,
                                                   shared_xref=self.latest_feed_xref, update_redis=True)

            self.kws.connect(threaded=True)

            while True:
                if datetime.now() > self.close_time:
                    logger.info(f'{self.name} Market Closed. Wrapping Up....')
                    # Stop candle creator
                    self.handler.stop_processor()
                    try:
                        cc = self.candle_fut.result(15)
                        cc.stop_processor()
                    except TimeoutError as exe:
                        logger.info(f'{self.name} Candle Creator Ended with timeout exception: {exe}')
                    except Exception as e:
                        logger.info(f'{self.name} Candle Creator Ended with unexpected exception: {e}')
                    # Stop Socket
                    self.kws.close(4500, 'Self Triggered')
                    self.kws.stop()
                    # Wrap up and exit
                    logger.info(f'Wrapping up {self.name}....')
                    # noinspection PyTypeChecker
                    # hist_fut = self.executor.submit(self.historical_etl_wrapper, kite_client=self.client, token_map=self.token_xref)
                    # logger.info(f'{self.name} Historical ETL status: {hist_fut.result()}')
                    # check_pids(source=self.name)
                    exit(30)
                else:
                    sleep(30)

    def ws_on_connect(self, ws, response):
        logger.info(f"{self.name} Websocket Connected: {response}")
        ws.set_mode(ws.MODE_FULL, self.tokens)
        # Connected and time more than open then add historical load
        # if today + Td.today_open_delta < datetime.now() < today + Td.market_close_delta():
        #     logger.info(f'{self.name} Adding Historical data load to queue')
        #     start_time = datetime.now() + Td.broadcast_candle_delta
        #     hist_fut_intra_day = self.executor.submit(self.historical_etl_wrapper, kite_client=self.client,
        #                                               token_map=self.token_xref, etl_start=start_time,
        #                                               flag_hist=self.hist_flag)
        #     logger.debug(hist_fut_intra_day)

    def ws_on_ticks(self, ws, ticks):
        try:
            self.handler.receiver(ticks)
        except Exception as e:
            logger.error(f'{self.name} Error in tick handling<{ws}>: {e}')

    def ws_on_close(self, ws, code, reason):
        logger.info(f'{self.name} Closed<{ws}>: {code}: {reason}')
        # ws.stop()  # Don't call stop if you want reconnect mechanism to kick in

    def ws_on_error(self, ws, code, reason):
        st = "*" * 50
        logger.info(f'{st} {self.name} Error in socket {st}')
        logger.error(f'{self.name} Error in Websocket<{ws}>. {code}: {reason}')

    # @staticmethod
    # def historical_etl_wrapper(kite_client, token_map, etl_start=datetime.now(), flag_hist=None):
    #     from utils import MINUTE
    #     run_opt_calc = True
    #     try:
    #         historical = HistoricalData(kite_client, token_map, start=today, end=today, interval=MINUTE, hist_flag=flag_hist,
    #                                     opt_calc=run_opt_calc, concurrent_limit=hist_threads)
    #         historical.start_etl(etl_start=etl_start, latest=True, daemon=False, timeout=10*60)
    #     except Exception as exc:
    #         logger.error(f'Error in historical ETL: {exc}\n{traceback.format_exc()}')
    #     logger.info(f'Historical ETL wrapper exists: {psutil.Process().pid} Parent: {psutil.Process().ppid()}')


def zws_wrapper(*args, **kwargs):
    try:
        zws = ZerodhaWS(*args, **kwargs)
        zws.start()
    except Exception as ec:
        logger.error(f'Error in ZWS Wrapper: {ec}')
