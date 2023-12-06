from concurrent.futures import ProcessPoolExecutor
from functools import partial
from multiprocessing import Manager, get_context, Queue

import requests

from common import logger
from xts_socket import MdSocketIO#, SocketIOHandler

user_id = 'BR052'
host = "https://algozy.rathi.com:3000"
# socket_url = f"wss://algozy.rathi.com:3000/marketdata/socket.io/"
socket_url = f"wss://algozy.rathi.com:3000"
access_token = ''
data_api_key = '9af31b94f3999bd12c6e89'
data_api_secret = 'Evas244$3H'
interactive_api_key = 'dabfe67ee2286b19a7b664'
interactive_api_secret = 'Mbqk087#Y1'


def login():
    url = f"{host}/apimarketdata/auth/login"
    payload = {"appKey": data_api_key, "secretKey": data_api_secret, "source": "WebAPI"}
    response = requests.post(url=url, json=payload)
    logger.info(response.content)
    data = response.json()
    return data


def subscribe_index():
    url = f"{host}/apimarketdata/instruments/subscription"
    payload = {"instruments": [{"exchangeSegment": 1, "exchangeInstrumentID": "26000"},
                               {"exchangeSegment": 2, "exchangeInstrumentID": "43202"},
                               {"exchangeSegment": 2, "exchangeInstrumentID": "43203"},
                               {"exchangeSegment": 2, "exchangeInstrumentID": "43224"},
                               {"exchangeSegment": 2, "exchangeInstrumentID": "43227"},
                               ],
               "xtsMessageCode": 1502}
    response = requests.post(url=url, headers={'authorization': access_token}, json=payload)
    logger.info(response.content)


def on_connect():
    subscribe_index()


def on_message(data, code=None):
    """On receiving message code 1502 full"""
    logger.info(f'{code} message: {data}')


def queue_processor(q: Queue):
    while True:
        try:
            msg = q.get()
            logger.info(msg)
        except Exception as q_exc:
            logger.error(f'Error in queue msg: {q_exc}')


on_message1501_json_full = partial(on_message, code=1501)
on_message1502_json_full = partial(on_message, code=1502)
on_message1507_json_full = partial(on_message, code=1507)
on_message1512_json_full = partial(on_message, code=1512)
# on_message1501_json_full = partial(on_message, code=1501)


def main():
    with ProcessPoolExecutor(max_workers=2, mp_context=get_context('spawn')) as executor:
        mp = Manager()
        queue = mp.Queue()
        client = MdSocketIO(url=host, token=access_token, userID=user_id)
        el = client.get_emitter()
        el.on('connect', on_connect)
        el.on('message', on_message)
        el.on('1501-json-full', on_message1501_json_full)
        el.on('1502-json-full', queue.put)
        # el.on('1507-json-full', on_message1507_json_full)
        # el.on('1512-json-full', on_message1512_json_full)
        # el.on('1105-json-full', on_message1105_json_full)

        executor.submit(queue_processor, queue)

        try:
            client.connect()
        except Exception as exc:
            logger.error(f"Error in connection: {exc}")


if __name__ == '__main__':
    info = login()
    access_token = info['result']['token']
    main()
