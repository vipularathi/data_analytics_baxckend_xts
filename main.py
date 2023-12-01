import os
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context, Manager, Pipe
from time import sleep

from common import logger
from contracts import get_instruments
from feed import connect_socket
from service import socket_url, access_token, subscribe_index, user_id, login
from zerodha import initiate_session, zws_wrapper


def get_req_contracts():
    scrips = ['NIFTY']
    ins = get_instruments()
    nse_ins = ins[ins['exchange'].isin(['NSE', 'NFO'])].copy()
    eq_filter = nse_ins['tradingsymbol'].isin(scrips)
    der_filter = (nse_ins['name'].isin(scrips)) & (nse_ins['expiry'] == '2023-12-07')
    req = nse_ins[eq_filter | der_filter].copy()
    tokens = req['instrument_token'].tolist()
    token_xref = req[['instrument_token', 'tradingsymbol']].set_index('instrument_token').to_dict()['tradingsymbol']
    return tokens, token_xref


def main():
    workers = max(os.cpu_count(), 4)
    logger.info(f'Max workers: {workers}. Main Pid: {os.getpid()}')
    with ProcessPoolExecutor(max_workers=workers, mp_context=get_context('spawn')) as executor:
        client = initiate_session()
        tokens, token_xref = get_req_contracts()
        logger.info(f'Entities for broadcast: {len(tokens)}')

        mp = Manager()
        # hist_flag = mp.Event()  # Moved to per instance
        # hist_flag.set()

        latest_feed_xref = mp.dict({_entity: {} for _token, _entity in token_xref.items()})  # Shared Among different

        # Initialize Pipe Objects
        candle_receiver, candle_send = Pipe(duplex=False)

        # # Initiate Candle Data Processor
        # names = [socket_name(_i) for _i in tokens_map.keys()]
        # # noinspection PyTypeChecker
        # data_processor = executor.submit(start_analysis, candle_receiver, tokens_map, xrefs, shared_xref=latest_feed_xref, names=names)  # NOSONAR

        # Initialize Broadcast
        # noinspection PyTypeChecker
        executor.submit(zws_wrapper, tokens, token_xref, [], client, candle_send,
                        name=1, latest_feed_xref=latest_feed_xref)

        while True:
            sleep(30)


if __name__ == '__main__':
    # ins = get_instruments()
    # login()
    # subscribe_index()
    # connect_socket(socket_url, access_token=access_token, user_id=user_id)
    # get_req_contracts()
    main()

