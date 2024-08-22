import pandas as pd
import numpy as np
import csv
import os
import sys
from datetime import date, datetime, time
from dateutil.relativedelta import relativedelta
import pytz
import requests

# from xts_connect import logger, access_token, host
from common import root_dir, today, yesterday, IST, logger
from remote_db_ops import master_df

# ------------------------------------------------------------------------------------------------
def update_expiry():
    master_df.rename(columns={'expiry_date':'expiry'}, inplace=True)
    # logger.info(f'\n master_sdf is \n{master_df}')

    current_month = datetime.now().month
    logger.info(f'\n current month is {current_month}')
    n_cur_exp, n_nxt_exp, n_nxt_nxt_exp = [], [], []
    bn_cur_exp, bn_nxt_exp = [], []
    fn_cur_exp, fn_nxt_exp = [], []
    mcn_cur_exp, mcn_nxt_exp = [], []
    for i in range(len(master_df)):
        master_dict_list = [master_df.iloc[i].to_dict()]
        master_dict = master_df.iloc[i]
    #     print(master_dict); print(master_dict.symbol, master_dict.expiry.month); print(master_dict_list,'\n')
        if str(master_dict.symbol).upper()=='NIFTY':
            if master_dict.expiry.month==current_month:
                n_cur_exp.append(master_dict.expiry)
            elif master_dict.expiry.month==(current_month+1)%12:
                n_nxt_exp.append(master_dict.expiry)
            elif master_dict.expiry.month == (current_month+2)%12:
                n_nxt_nxt_exp.append(master_dict.expiry)
        elif str(master_dict.symbol).upper()=="BANKNIFTY":
            if master_dict.expiry.month==current_month:
                bn_cur_exp.append(master_dict.expiry)
            elif master_dict.expiry.month==(current_month+1)%12:
                bn_nxt_exp.append(master_dict.expiry)
        elif str(master_dict.symbol).upper()=="FINNIFTY":
            if master_dict.expiry.month==current_month:
                fn_cur_exp.append(master_dict.expiry)
            elif master_dict.expiry.month==(current_month+1)%12:
                fn_nxt_exp.append(master_dict.expiry)
        elif str(master_dict.symbol).upper()=="MIDCPNIFTY":
            if master_dict.expiry.month==current_month:
                mcn_cur_exp.append(master_dict.expiry)
            elif master_dict.expiry.month==(current_month+1)%12:
                mcn_nxt_exp.append(master_dict.expiry)
    # print(n_cur_exp)
    # nifty_exp = n_cur_exp+n_nxt_exp+n_nxt_nxt_exp
    # bn_exp = bn_cur_exp+bn_nxt_exp
    # fn_exp = fn_cur_exp+fn_nxt_exp
    # mcn_exp = mcn_cur_exp+mcn_nxt_exp
    # print(f'\n NIFTY exp are {sorted(nifty_exp)}\n BANKNIFTY exp are {bn_exp}\n FINNIFTY exp are {fn_exp}\n MIDCPNIFTY exp are {mcn_exp}')

    # nifty_exp1 = []
    # bn_exp1 = []
    # fn_exp1 = []
    # mcn_exp1 = []

    #----------------------------------------------------------------
    if len(n_cur_exp) == 2:
        nifty_exp1 = n_cur_exp + [n_nxt_exp[-1]] + [n_nxt_nxt_exp[-1]]
    elif len(n_cur_exp) == 1:
        nifty_exp1 = n_cur_exp +[n_nxt_exp[0]] + [n_nxt_exp[-1]] + [n_nxt_nxt_exp[-1]]
    elif len(n_cur_exp) == 0:
        nifty_exp1 = n_nxt_exp[:2] + [n_nxt_exp[-1]] + [n_nxt_nxt_exp[-1]]
    else:
        nifty_exp1 = n_cur_exp[:2] + [n_cur_exp[-1]] + [n_nxt_exp[-1]]
    #----------------------------------------------------------------
    if len(bn_cur_exp) == 1:
        bn_exp1 = bn_cur_exp + [bn_nxt_exp[0]]
    elif len(bn_cur_exp) == 0:
        bn_exp1 = bn_nxt_exp[:2]
    else:
        bn_exp1 = bn_cur_exp[:2]
    #----------------------------------------------------------------
    if len(fn_cur_exp) == 1:
        fn_exp1 = fn_cur_exp + [fn_nxt_exp[0]]
    elif len(fn_cur_exp) == 0:
        fn_exp1 = fn_nxt_exp[:2]
    else:
        fn_exp1 = fn_cur_exp[:2]
    #----------------------------------------------------------------
    if len(mcn_cur_exp) == 1:
        mcn_exp1 = mcn_cur_exp + [mcn_nxt_exp[0]]
    elif len(mcn_cur_exp) == 0:
        mcn_exp1 = mcn_nxt_exp[:2]
    else:
        mcn_exp1 = mcn_cur_exp[:2]
    #----------------------------------------------------------------
    logger.info(f'\n NIFTY exp are {sorted(nifty_exp1)}\n BANKNIFTY exp are {bn_exp1}\n FINNIFTY exp are {fn_exp1}\n MIDCPNIFTY exp are {mcn_exp1}')
    final_exp = nifty_exp1 + bn_exp1 + fn_exp1 + mcn_exp1
    # print(f'\nfinal exp is {final_exp}')
    new_final_exp = [each.strftime('%d/%m/%Y') for each in final_exp]
    # print('\n final exp is', new_final_exp)

    data = {
        'symbol' : ['NIFTY', 'NIFTY', 'NIFTY', 'NIFTY', 'BANKNIFTY', 'BANKNIFTY',
                    'FINNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'MIDCPNIFTY'],
        'expiry' : new_final_exp
    }
    logger.info('\n', data)
    df = pd.DataFrame(data)
    df['expiry'] = pd.to_datetime(df['expiry'], dayfirst = True)
    grouped_df = df.groupby(['symbol']).agg({'expiry':list})
    exploded_df = df.explode('expiry')

    df.to_excel(os.path.join(root_dir, 'symbols.xlsx'), index = False)
    logger.info('\n expiry file updated')
    print('done')
    return(df)

# res = update_expiry()
# print(res)