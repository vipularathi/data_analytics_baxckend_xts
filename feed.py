import websocket

from common import logger
from xts_connect import subscribe_index


class Feed(websocket.WebSocketApp):
    pass


def on_open(ws: Feed):
    logger.info(f'open {ws}')
    subscribe_index()


def on_message(ws: Feed, data):
    logger.info(f'message {data}')


def on_data(ws: Feed, data, op_code, cont):
    logger.info(f'data {data} {op_code} {cont}')


def on_close(ws: Feed, status_code, msg):
    logger.info(f'close {status_code} {msg}')


def on_error(ws: Feed, exc):
    logger.info(f'error {exc}')


def on_ping(ws, msg):
    logger.info(f"ping {msg}")


def on_pong(ws, msg):
    logger.info(f"pong {msg}")


def connect_socket(url, user_id, access_token):
    feed_url = f"{url}?token={access_token}&userID={user_id}&publishFormat=JSON&broadcastMode=Full&transport=websocket"
    print(feed_url)
    ws = Feed(feed_url, on_open=on_open, on_message=on_message, on_close=on_close, on_error=on_error,
              on_ping=on_ping, on_pong=on_pong)
    ws.run_forever(ping_interval=5, reconnect=5)
    logger.info('exists')
