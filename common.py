import logging
import os
import sys
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler

import pandas as pd
import pytz
from dateutil.relativedelta import relativedelta

root_dir = os.path.dirname(os.path.abspath(__file__))
logs_dir = os.path.join(root_dir, 'logs/')
data_dir = os.path.join(root_dir, 'entity_data/')
dirs = [logs_dir, data_dir]
# dirs = [logs_dir, provider_dir]
status = [os.makedirs(_dir, exist_ok=True) for _dir in dirs if not os.path.exists(_dir)]
instruments_path = os.path.join(data_dir, 'instruments.csv')  # Unique for each day. No History available.

holidays_23 = ['2023-01-26', '2023-03-07', '2023-03-30', '2023-04-04', '2023-04-07', '2023-04-14', '2023-05-01', '2023-06-29', '2023-08-15', '2023-09-19', '2023-10-02', '2023-10-24', '2023-11-14', '2023-11-27', '2023-12-25']
holidays_24 = ['2024-01-22', '2024-01-26', '2024-03-08', '2024-03-25', '2024-03-29', '2024-04-11', '2024-04-17', '2024-05-01', '2024-06-17', '2024-07-17', '2024-08-15', '2024-10-02', '2024-11-01', '2024-11-15', '2024-12-25']
holidays = holidays_23 + holidays_24  # List of date objects
b_days = pd.bdate_range(start=datetime.now()-relativedelta(months=3), end=datetime.now(), freq='C', weekmask='1111100',
                        holidays=holidays)
b_days = b_days.append(pd.DatetimeIndex([pd.Timestamp(year=2024, month=1, day=20)]))
b_days = b_days[b_days <= datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)].drop_duplicates().sort_values()
today, yesterday = b_days[-1], b_days[-2]
IST = pytz.timezone('Asia/Kolkata')


def define_logger():
    # Logging Definitions
    log_lvl = logging.DEBUG
    console_log_lvl = logging.INFO
    _logger = logging.getLogger('arathi')
    # logger.setLevel(log_lvl)
    _logger.setLevel(console_log_lvl)
    log_file = os.path.join(logs_dir, f'logs_arathi_{datetime.now().strftime("%Y%m%d")}.log')
    handler = TimedRotatingFileHandler(log_file, when='D', delay=True)
    handler.setLevel(log_lvl)
    console = logging.StreamHandler(stream=sys.stdout)
    console.setLevel(console_log_lvl)
    # formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')  #NOSONAR
    # formatter = logging.Formatter('%(asctime)s %(levelname)s %(filename)s %(funcName)s %(message)s')
    formatter = logging.Formatter('%(asctime)s %(levelname)s <%(funcName)s> %(message)s')
    handler.setFormatter(formatter)
    console.setFormatter(formatter)
    _logger.addHandler(handler)  # Comment to disable file logs
    _logger.addHandler(console)
    # logger.propagate = False  # Removes AWS Level Logging as it tracks root propagation as well
    return _logger


logger = define_logger()
