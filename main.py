import os
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context, Manager, Pipe
from time import sleep

from analysis import start_analysis
from common import logger
from contracts import get_req_contracts
from feed import connect_socket
from xts_connect import socket_url, access_token, subscribe_index, user_id, login
from zerodha import initiate_session, zws_wrapper


def main():
    workers = max(os.cpu_count(), 4)
    logger.info(f'Max workers: {workers}. Main Pid: {os.getpid()}')
    with ProcessPoolExecutor(max_workers=workers, mp_context=get_context('spawn')) as executor:
        client = initiate_session()
        ins_df, tokens, token_xref = get_req_contracts()
        logger.info(f'Entities for broadcast: {len(tokens)}')

        mp = Manager()
        # hist_flag = mp.Event()  # Moved to per instance
        # hist_flag.set()

        latest_feed_xref = mp.dict({_entity: {} for _token, _entity in token_xref.items()})  # Shared Among different

        # Initialize Pipe Objects
        candle_receiver, candle_send = Pipe(duplex=False)

        # Initiate Candle Data Processor
        # noinspection PyTypeChecker
        data_processor = executor.submit(start_analysis, ins_df, tokens, token_xref, shared_xref=latest_feed_xref)  # NOSONAR

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

