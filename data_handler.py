import traceback
from datetime import datetime, timedelta
from functools import cached_property
from queue import Queue
from threading import Thread
from time import sleep

import psutil

from common import logger


epoch_to_datetime = lambda epoch_time: datetime.combine(datetime.today().date(), (datetime.fromtimestamp(epoch_time) - timedelta(hours=5, minutes=30)).time())


class DataHandler:
    def __init__(self, sender=None) -> None:
        super().__init__()
        self.queue = Queue()
        self.processor_thread = None
        self._kill = False
        self.sender = sender if sender is not None else None

    @cached_property
    def _do_send(self):
        return True if self.sender is not None else False

    def start_processor(self):
        logger.info(f'Data Handler Initiated')
        th = Thread(target=self._data_processor, name='Data Processor')
        th.start()
        self.processor_thread = th

    def stop_processor(self):
        self._kill = True
        if self.processor_thread is not None:
            self.processor_thread.join()
        logger.info('Data Handler Exits')

    def receiver(self, data):
        self.queue.put(data)

    def _data_processor(self):
        while True:
            if self.queue.qsize() > 0:
                _feed = self.queue.get()
                # Change the feed format here
                _f_feed = _feed
                # logger.info(f'Data Handler: {len(_feed)}')
                # Send the feed after formatting, if required
                if self._do_send:
                    self.sender.send(_f_feed)
                else:
                    logger.debug(_f_feed)
            else:
                sleep(0.07)

            if self._kill is True:
                break

        logger.debug(f'Data Processor Exit')


class CandleCreator:

    def __init__(self, scrips: list, tokens, token_xref, receiver, start=False, **kwargs) -> None:
        """
        Initializer for the candle creation
        :param scrips: List[Scrip]
                List of scrips for whom data is to be supplied.
        :param receiver: Connection
                Pipe interface to receive the real-time data/messages from broadcast process
        :param kwargs: To accept key-value pairs into the function.
        """
        super().__init__()
        self.scrips = scrips
        self.receiver = receiver
        self.tokens = tokens
        self.token_xref = token_xref  # token -> entity
        self.entity_xref = {_v: _k for _k, _v in token_xref.items()}  # entity -> token
        self.entities = list(self.token_xref.values())
        self.scrip_xref = {_scrip.entity: _scrip for _scrip in self.scrips}
        self.shared_xref = kwargs.get('shared_xref', {})
        self.name = kwargs.get('name', 'sock')
        self.mode = kwargs.get('mode', 'zerodha')

        self.candle_sender = kwargs.get('c_sender', None)
        self.daemon = kwargs.get('threaded', False)

        self._token_vol = {_token: 0 for _token in self.tokens}
        self._recv_queue = Queue()  # Receiver Queue
        self._ltp_queue = Queue()  # LTP Queue
        self._redis_queue = Queue()
        self._candle_queue = Queue()  # Candle Queue
        self._kill = False  # To initiate Kill all threads
        self._th_receive = None
        self._th_process = None
        self._th_ltp = None
        self._th_candle = None
        self._th_redis = None
        self._freq_threads = []

        self._is_first = True

        self._eod_df_no_vol, self._latest_xref = None, None

        if start:
            self.start_processor(self.daemon)

    @cached_property
    def key_fmt(self):
        return '%H%M'

    @cached_property
    def do_send(self):
        return True if self.candle_sender is not None else False

    def start_processor(self, daemon=False):
        logger.debug(f'Initiating Candle Creator....')
        self._th_receive = Thread(target=self._data_receiver, daemon=daemon, name='CC: Data Receiver')
        self._th_receive.start()
        self._th_process = Thread(target=self._data_processor, daemon=daemon, name='CC: Data Processor')
        self._th_process.start()

        logger.info(f'Candle Creator Initiated')

    def stop_processor(self):
        logger.info(f'Candle Creator Stop initiated')
        self._kill = True
        if self._th_process is not None:
            self._th_process.join()
        if self._th_receive is not None:
            self._th_receive.join()
        if self._th_ltp is not None:
            self._th_ltp.join()
        if self._th_candle is not None:
            self._th_candle.join()
        logger.info(f'Candle Creator Exits')

    def _data_receiver(self):
        while True:
            _feed = self.receiver.recv()
            self._recv_queue.put(_feed)
            if self._kill is True:
                break

    # NOSONAR
    def _data_processor(self):
        while True:
            try:
                if self._recv_queue.qsize() > 0:
                    _feed = self._recv_queue.get()
                    # logger.info(f'Candle Creator: {len(_feed)}')
                    if len(_feed) > 0:
                        for _e_feed in _feed:
                            _data, _xref_dict = self._extract_connector_feed(_e_feed)
                            # self._ltp_queue.put(_data)
                            self._update_shared_xref(_xref_dict)
                        # logger.info(self.shared_xref.copy())
                else:
                    sleep(0.07)

                if self._kill is True:
                    break
            except Exception as exc:
                logger.error(f'Error while processing feed: {exc}')

    def _extract_connector_feed(self, entity_feed):
        if self.mode == 'zerodha':
            return self._extract_feed_v4(entity_feed)
        elif self.mode == 'xts':
            return self._extract_feed_xts(entity_feed)
        else:
            logger.error('feed mode not understood')
            return [], {}

    def _extract_feed_v4(self, entity_feed):
        keys = entity_feed.keys()
        # slot, token, last_price, vol, cum_vol, oi
        feed = [entity_feed['exchange_timestamp'].strftime(self.key_fmt), entity_feed['instrument_token'],
                entity_feed['last_price'],
                entity_feed['last_traded_quantity'] if 'last_traded_quantity' in keys else 0,  # For Index
                entity_feed['volume_traded'] if 'volume_traded' in keys else 0,  # For Index
                entity_feed['oi'] if 'oi' in keys else 0,  # For Index
                {'prev_close': entity_feed['ohlc']['close'], 'chg': entity_feed['change'],
                 'ts': int(entity_feed['exchange_timestamp'].timestamp() * 1000)}
                ]

        return feed, entity_feed

    def _extract_feed_xts(self, entity_feed):
        # logger.info(f"extract {entity_feed}")
        # {'MessageCode': 1502, 'MessageVersion': 4, 'ApplicationType': 0, 'TokenID': 0, 'ExchangeSegment': 2, 'ExchangeInstrumentID': 68094, 'ExchangeTimeStamp': 1396448201, 'Bids': [{'Size': 250, 'Price': 518, 'TotalOrders': 2, 'BuyBackMarketMaker': 0}, {'Size': 450, 'Price': 517.95, 'TotalOrders': 2, 'BuyBackMarketMaker': 0}, {'Size': 100, 'Price': 517.9, 'TotalOrders': 1, 'BuyBackMarketMaker': 0}, {'Size': 50, 'Price': 517.85, 'TotalOrders': 1, 'BuyBackMarketMaker': 0}, {'Size': 500, 'Price': 517.8, 'TotalOrders': 3, 'BuyBackMarketMaker': 0}], 'Asks': [{'Size': 100, 'Price': 519.5, 'TotalOrders': 1, 'BuyBackMarketMaker': 0}, {'Size': 700, 'Price': 519.55, 'TotalOrders': 2, 'BuyBackMarketMaker': 0}, {'Size': 250, 'Price': 519.6, 'TotalOrders': 2, 'BuyBackMarketMaker': 0}, {'Size': 400, 'Price': 519.65, 'TotalOrders': 1, 'BuyBackMarketMaker': 0}, {'Size': 100, 'Price': 519.7, 'TotalOrders': 1, 'BuyBackMarketMaker': 0}], 'Touchline': {'BidInfo': {'Size': 250, 'Price': 518, 'TotalOrders': 2, 'BuyBackMarketMaker': 0}, 'AskInfo': {'Size': 100, 'Price': 519.5, 'TotalOrders': 1, 'BuyBackMarketMaker': 0}, 'LastTradedPrice': 518.75, 'LastTradedQunatity': 50, 'TotalBuyQuantity': 28750, 'TotalSellQuantity': 30300, 'TotalTradedQuantity': 353500, 'AverageTradedPrice': 534.55, 'LastTradedTime': 1396448190, 'LastUpdateTime': 1396448201, 'PercentChange': 8.775424617320194, 'Open': 456.9, 'High': 568.45, 'Low': 456.9, 'Close': 476.9, 'TotalValueTraded': None, 'BuyBackTotalBuy': 0, 'BuyBackTotalSell': 0}, 'BookType': 1, 'XMarketType': 1, 'SequenceNumber': 1341194566648050, 'entity': 'NIFTY24APR22200CE', 'oi': None}
        touchline = entity_feed.get('Touchline', {})
        ins_token = self.entity_xref[entity_feed['entity']]
        feed = [epoch_to_datetime(entity_feed['ExchangeTimeStamp']).strftime(self.key_fmt),
                ins_token, touchline['LastTradedPrice'],
                touchline.get('LastTradedQunatity', 0), touchline.get('TotalTradedQuantity', 0),
                entity_feed.get('oi', 0), {'prev_close': None, 'chg': None, 'ts': None}]
        xref = {'instrument_token': ins_token, 'last_price': feed[2], 'oi': feed[5]}
        # logger.info(f"extracted {feed}")
        return feed, xref

    def _update_shared_xref(self, feed):
        self.shared_xref[self.token_xref[feed['instrument_token']]] = feed


def init_candle_creator(scrips, tokens, token_xref, feed_receiver, start=True, candle_sender=None,
                        threaded=False, hist_flag=None, **kwargs):
    try:
        cc = CandleCreator(scrips=scrips, tokens=tokens, token_xref=token_xref, receiver=feed_receiver,
                           start=start, c_sender=candle_sender, threaded=threaded, hist_flag=hist_flag, **kwargs)
        logger.info(f'Candle Creator Process running at pid: {psutil.Process().pid}')
        return cc
    except Exception as exc:
        logger.error(f'Error while init Candle Creator: {exc}\n{traceback.format_exc()}')
