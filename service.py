import requests

from common import logger

user_id = 'BR052'
host = "https://algozy.rathi.com:3000"
socket_url = f"wss://algozy.rathi.com:3000/marketdata/socket.io/"
access_token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySUQiOiJCUjA1Ml9EQUJGRTY3RUUyMjg2QjE5QTdCNjY0IiwicHVibGljS2V5IjoiZGFiZmU2N2VlMjI4NmIxOWE3YjY2NCIsImlhdCI6MTcwMTM5OTcyMSwiZXhwIjoxNzAxNDg2MTIxfQ.WOIrqNbZVuRr4mpLirxCw1uo-WLZyXmri6O1nuKiEc4'
data_api_key = '9af31b94f3999bd12c6e89'
data_api_secret = 'Evas244$3H'
interactive_api_key = 'dabfe67ee2286b19a7b664'
interactive_api_secret = 'Mbqk087#Y1'

# user_id = 'S1670'
# host = "https://trade.sscorporate.com:3000"
# socket_url = f"wss://trade.sscorporate.com:3000/feedmarketdata/socket.io/"
# access_token = ''
# data_api_key = 'bc62eaaee74e65d73c2966'
# data_api_secret = 'Kxfj648$46'
# interactive_api_key = '7c3112216aa9a255ae1719'
# interactive_api_secret = 'Wnnk815@Bq'


def login():
    url = f"{host}/interactive/user/session"
    payload = {"appKey": interactive_api_key, "secretKey": interactive_api_secret, "source": "WebAPI"}
    response = requests.post(url=url, headers={'authorization': access_token}, json=payload)
    logger.info(response.content)


def subscribe_index():
    url = f"{host}/marketdata/instruments/subscription"
    payload = {"instruments": [{"exchangeSegment": 1, "exchangeInstrumentID": "26000"}], "xtsMessageCode": 1502}
    response = requests.post(url=url, headers={'authorization': access_token}, json=payload)
    logger.info(response.content)
