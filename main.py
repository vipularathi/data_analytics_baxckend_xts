import os
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context, Manager, Pipe
from time import sleep

import xts_main
from analysis import start_analysis
from common import logger
from contracts import get_req_contracts
from zerodha import initiate_session, zws_wrapper


def main():
    choice = str(input("Enter broker(zerodha/XTS): ")).lower()

    workers = max(os.cpu_count(), 4)
    logger.info(f'Max workers: {workers}. Main Pid: {os.getpid()}')

    with ProcessPoolExecutor(max_workers=workers, mp_context=get_context('spawn')) as executor:
        if choice == "xts":
            access_tokens, headers, userids, ch = xts_main.get_token_header()  # connection to XTS - return access tokens, headers, userid, choice(subs/unsubs)

        else:
            client = initiate_session()     # connection to zerodha kite

        ins_df, tokens, token_xref = get_req_contracts()
        logger.info(f'Entities for broadcast: {len(tokens)}')

        mp = Manager()
        # hist_flag = mp.Event()  # Moved to per instance
        # hist_flag.set()

        # common dictionary
        latest_feed_xref = mp.dict({_entity: {} for _token, _entity in token_xref.items()})  # Shared Among different

        # Initialize Pipe Objects
        candle_receiver, candle_send = Pipe(duplex=False)

        # Initiate Candle Data Processor
        # noinspection PyTypeChecker
        data_processor = executor.submit(start_analysis, ins_df, tokens, token_xref, shared_xref=latest_feed_xref)  # NOSONAR

        # Initialize Broadcast
        if choice == "xts":

            # divide entities into tokens
            ins_df_list = xts_main.split_into_tokens(access_tokens, ins_df)     # ins_df_list[i] is a Dataframe
            xts_main.subscribe_init(tokens=access_tokens, headers=headers, ch=ch, df=ins_df_list)  # subscribe

            xts_token_xref = ins_df.set_index('exchange_token')['tradingsymbol'].to_dict()
            if ch == 'subs':
                for i in range(len(access_tokens)):
                    # push data for each token
                    executor.submit(xts_main.xts_wrapper, tokens, token_xref, [], access_tokens[i], userids[i], candle_send,
                                    latest_feed_xref=latest_feed_xref, xts_token_xref=xts_token_xref)

        else:
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

