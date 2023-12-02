import json
import traceback
from datetime import datetime
from time import sleep

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from colorama import Style, Fore
from dateutil.relativedelta import relativedelta

from common import logger, today
from db_ops import DBHandler


class SnapAnalysis:

    def __init__(self, ins_df, tokens, token_xref, shared_xref):
        super().__init__()
        self.ins_df = ins_df
        self.tokens = tokens
        self.token_xref = token_xref
        self.shared_xref = shared_xref

        self.scheduler = None
        self.scheduler: BackgroundScheduler = self.init_scheduler()
        self.scheduler.start()  # Scheduler starts

    def init_scheduler(self):
        if self.scheduler is not None:
            return self.scheduler

        executors = {
            'default': {'type': 'threadpool', 'max_workers': 5}
        }

        job_defaults = {
            'coalesce': True,
            'max_instances': 1,
            'misfire_grace_time': 10
        }
        self.scheduler = BackgroundScheduler(executors=executors, job_defaults=job_defaults, timezone='Asia/Kolkata',
                                             logger=logger)
        self.add_jobs_to_scheduler()
        return self.scheduler

    def add_jobs_to_scheduler(self):
        mkt_open = today + relativedelta(hour=9, minute=15)
        mkt_close = today + relativedelta(hour=15, minute=30)

        # Add jobs
        trigger_snap = IntervalTrigger(minutes=1, start_date=mkt_open, end_date=mkt_close)
        self.scheduler.add_job(self.run_analysis, trigger_snap, args=(), id='snap_1',
                               name=f'SnapDataAnalysis')

    def run_analysis(self):
        dt = datetime.now(tz=pytz.timezone('Asia/Kolkata'))
        xref = self.shared_xref.copy()
        logger.info(list(xref.keys()))
        data = {'timestamp': dt.isoformat(), 'snap': xref}
        DBHandler.insert_snap_data([json.loads(json.dumps(data, default=str))])


def start_analysis(ins_df, tokens, token_xref, shared_xref):
    try:
        SnapAnalysis(ins_df, tokens, token_xref, shared_xref)

        while True:
            sleep(10)
    except Exception as exc:
        logger.error(f'{Style.BRIGHT}{Fore.GREEN}Error in initiating Strategy Data Processor: {exc}\n{"*" * 15} Requires immediate attention {"*" * 15}{Style.RESET_ALL}')
        logger.error(traceback.format_exc())
