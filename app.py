from datetime import date, datetime, time
from itertools import zip_longest
import numpy as np
import pandas as pd
import uvicorn
from fastapi import FastAPI, Query, status
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from common import IST, yesterday, today, logger, fixed_response_dict, round_spot, read_symbols
from contracts import get_req_contracts
from db_ops import DBHandler


class ServiceApp:

    def __init__(self):
        super().__init__()
        self.app = FastAPI(title='ARathi', description='ARathi', docs_url='/docs', openapi_url='/openapi.json')
        self.app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"],
                                allow_headers=["*"])

        self.add_routes()
        self.symbol_expiry_map = None
        self.use_otm_iv = True
        self.copy_symbol_expiry_map = None
        self.copy_symbol_expiry_spot = None
        self.list_dict = []
        self.difference = None

    def add_routes(self):
        self.app.add_api_route('/', methods=['GET'], endpoint=self.default)
        self.app.add_api_route('/symbol', methods=['GET'], endpoint=self.get_symbols)
        self.app.add_api_route('/straddle/minima', methods=['GET'], endpoint=self.fetch_straddle_minima)
        self.app.add_api_route('/straddle/minima/table', methods=['GET'],
                               endpoint=self.fetch_straddle_minima_table)  # NEW
        self.app.add_api_route('/straddle/iv', methods=['GET'], endpoint=self.fetch_straddle_iv)
        self.app.add_api_route('/straddle/cluster', methods=['GET'], endpoint=self.fetch_straddle_cluster)
        self.app.add_api_route('/login', methods=['POST'], endpoint=self.userLogin)

    @staticmethod
    def default():
        body = {'status': 'success', 'message': '', 'data': [], 'response_code': None}
        return JSONResponse(content=jsonable_encoder(body), status_code=status.HTTP_200_OK)

    def isUserExist(self, username, password):
        msg, data = DBHandler.check_user_exist(username)
        if msg:
            # if not data.get("active", False):
            #     return False, "user is inactive"

            if data['email'] == 'test@rathi.com' and data['password'] in ['test']:
                return True, data

            # hashed_password = data.get("pwd", '')
            # if not pwd_context.verify(user.password, hashed_password):
            #     return False, 'Incorrect Password'

            return True, data
        else:
            return False, "User not exist"

    def userLogin(self, username, password) -> dict:
        '''
        /login - POST - user sends username and password and it is verified and
         if successful then send role and token in the response.
         (Token generated in this step to be stored in the token table)
        '''
        # check the user exists or not in user table
        msg, res = self.isUserExist(username, password)

        if msg and res['email'] == username and res['password'] == password:
            return {"msg": True, "output": "login success"}
        else:
            return {"msg": True, "output": "login failure"}

    def get_symbols(self):
        if self.symbol_expiry_map is None:
            ins_df, tokens, token_xref = get_req_contracts()
            ins_df['expiry'] = ins_df['expiry'].dt.strftime('%Y-%m-%d')
            agg = ins_df[ins_df['instrument_type'].isin(['CE', 'PE'])].groupby(['name'], as_index=False).agg({'expiry': set, 'tradingsymbol': 'count'})
            agg['expiry'] = agg['expiry'].apply(lambda x: sorted(list(x)))
            self.symbol_expiry_map = agg.to_dict('records')
            symbol_expiry_map = agg.to_dict('records')
            logger.info(f'\n symbol expiry map is \n{symbol_expiry_map} \n and type is {type(symbol_expiry_map)}')
            self.copy_symbol_expiry_map = self.symbol_expiry_map.copy()
        return self.symbol_expiry_map

    def fetch_straddle_minima(self, symbol: str = Query(), expiry: date = Query(), st_cnt: int = Query(default=None), interval: int = Query(1), cont: bool = Query(False)):
        if cont:
            df_orig = DBHandler.get_straddle_minima(symbol, expiry, start_from=yesterday)
            logger.info(f'\n{symbol} {expiry} {cont} {today} {yesterday} df orig is \n {df_orig}')

            df_orig['prev'] = df_orig['ts'] < today
            logger.info(f'\n{symbol} {expiry} df orig prev is \n {df_orig}')

            df_yest = df_orig[df_orig['prev']==True].copy()
            logger.info(f'\n {symbol} {expiry} df_yest is \n {df_yest}')

            df_today = df_orig[df_orig['prev']==False].copy()
            logger.info(f'\n {symbol} {expiry} df_today is \n {df_today}')

        else:
            df_yest = pd.DataFrame()
            df_today = DBHandler.get_straddle_minima(symbol, expiry)
            df_today['prev'] = False

        fixed_resp = fixed_response_dict()
        fixed_df = pd.DataFrame(fixed_resp)
        # # logger.info(f'\nfixed df is \n{fixed_df.head()}')
        # df_yest['ts'] = pd.to_datetime(df_yest['ts'])
        df_today['ts'] = pd.to_datetime(df_today['ts'])
        fixed_df['ts'] = pd.to_datetime(fixed_df['ts'])
        # logger.info(f'changed ts in all 3 df')

        # # mtd-1
        merged_df = pd.merge(fixed_df, df_today, on='ts', how='left', suffixes = ('', '_y'))
        logger.info(f'\n{symbol} {expiry} orig merged df is \n{merged_df}')

        # Fill missing values
        col_list = ['spot', 'strike', 'combined_premium', 'combined_iv', 'otm_iv', 'prev']
        for col in col_list:
            merged_df[col] = merged_df[col].fillna(merged_df[f'{col}_y']).fillna(0)
        logger.info(f'\n{symbol} {expiry} merged df 1 is \n{merged_df}')
        merged_df.drop(columns=col_list, axis=1, inplace=True)
        logger.info(f'\n{symbol} {expiry} merged df 2 is \n{merged_df}')
        rename_dict = {f'{col}_y': col for col in col_list}
        merged_df.rename(columns=rename_dict, inplace=True)
        merged_df.fillna(0, inplace=True)
        merged_df['prev'] = False
        logger.info(f'\n{symbol} {expiry} updated merged df is \n{merged_df}')


        final_df = pd.concat([df_yest, merged_df], axis=0)
        logger.info(f'\n{symbol} {expiry} final df is \n{final_df}')
        # # logger.info(f'merged df {symbol} {expiry} after updation is \n{final_df}')
        final_df.reset_index(drop=True, inplace=True)

        if self.use_otm_iv:
            final_df['combined_iv'] = final_df['otm_iv']
        return self._straddle_response(final_df, count=st_cnt, interval=interval, sym= symbol, exp=str(expiry))

    def fetch_straddle_minima_table(self, st_cnt: int = Query(default=None), interval: int = Query(1),
                                    cont: bool = Query(False), table: bool = Query(True)):
        if self.copy_symbol_expiry_map:
            # # logger.info(f'\nsym exp map is {self.copy_symbol_expiry_map}')
            for_table = []
            current_time = datetime.now().time()
            if current_time > time(9,15):
                for i in range(len(self.copy_symbol_expiry_map)):
                    name = self.copy_symbol_expiry_map[i]['name']
                    sorted_exp = sorted(self.copy_symbol_expiry_map[i]['expiry'])
                    if name == 'NIFTY':
                        new_exp = sorted_exp[:2]
                        dict_1 = {'NIFTY_CW': new_exp[0], 'NIFTY_NW': new_exp[1]}
                        for_table.append(dict_1)
                    elif name == 'BANKNIFTY':
                        new_exp = sorted_exp[:2]
                        dict_1 = {'BANKNIFTY_CW': new_exp[0], 'BANKNIFTY_NW': new_exp[1]}
                        for_table.append(dict_1)
                    elif name == 'FINNIFTY':
                        dict_1 = {'FINNIFTY': sorted_exp[0]}
                        for_table.append(dict_1)
                    else:
                        new_exp = sorted_exp[0]
                        dict_1 = {'MIDCPNIFTY': new_exp}
                        for_table.append(dict_1)

                logger.info(f'\n for table dict is \n{for_table}')

                final_json = []
                for i in for_table:
                    for symbol, expiry in i.items():
                        if symbol.startswith('NIFTY'):
                            symbol1 = 'NIFTY'
                        elif symbol.startswith('BANK'):
                            symbol1 = 'BANKNIFTY'
                        elif symbol.startswith('FIN'):
                            symbol1 = 'FINNIFTY'
                        else:
                            symbol1 = 'MIDCPNIFTY'
                        # # logger.info(f'\n changed key is {symbol1} and value is {expiry}')
                        list_dict_resp = DBHandler.get_straddle_minima_table(symbol1, expiry)
                        new_dict = {symbol: list_dict_resp}
                        # # logger.info(f'\nnew_dict is {new_dict}')
                        final_json.append(new_dict)
                        # # logger.info(f'\nmaking final json resp- {final_json}')
                return final_json
            else:
                # return None
                empty_json = [
                        {
                            "BANKNIFTY_CW": [
                                {
                                    "Live": 0,
                                    "Live-Min": 0,
                                    "Max-Live": 0,
                                    "Max": 0,
                                    "Min": 0
                                }
                            ]
                        },
                        {
                            "BANKNIFTY_NW": [
                                {
                                    "Live": 0,
                                    "Live-Min": 0,
                                    "Max-Live": 0,
                                    "Max": 0,
                                    "Min": 0
                                }
                            ]
                        },
                        {
                            "FINNIFTY": [
                                {
                                    "Live": 0,
                                    "Live-Min": 0,
                                    "Max-Live": 0,
                                    "Max": 0,
                                    "Min": 0
                                }
                            ]
                        },
                        {
                            "MIDCPNIFTY": [
                                {
                                    "Live": 0,
                                    "Live-Min": 0,
                                    "Max-Live": 0,
                                    "Max": 0,
                                    "Min": 0
                                }
                            ]
                        },
                        {
                            "NIFTY_CW": [
                                {
                                    "Live": 0,
                                    "Live-Min": 0,
                                    "Max-Live": 0,
                                    "Max": 0,
                                    "Min": 0
                                }
                            ]
                        },
                        {
                            "NIFTY_NW": [
                                {
                                    "Live": 0,
                                    "Live-Min": 0,
                                    "Max-Live": 0,
                                    "Max": 0,
                                    "Min": 0
                                }
                            ]
                        }
                    ]
                return empty_json

    def fetch_straddle_iv(self, symbol: str = Query(), expiry: date = Query(), st_cnt: int = Query(default=None),
                          interval: int = Query(5)):
        df = DBHandler.get_straddle_iv_data(symbol, expiry)
        if self.use_otm_iv:
            df['combined_iv'] = df['otm_iv']
        return self._straddle_response(df, count=st_cnt, interval=interval)

    def fetch_straddle_cluster(self, symbol: str = Query(), expiry: date = Query(), st_cnt: int = Query(default=10), interval: int = Query(5)):
        all_df = DBHandler.get_straddle_iv_data(symbol, expiry, start_from=yesterday)
        all_data = []
        today_df = all_df[all_df['ts'] >= today].copy()
        prev_df = all_df[all_df['ts'] < today].copy()
        if len(prev_df):
            max_ts = prev_df['ts'].max()
            prev_df = prev_df[prev_df['ts'] == max_ts].copy()
            all_data.append(prev_df)
        if len(today_df):
            all_data.append(today_df)

        if all_data:
            df = pd.concat(all_data, ignore_index=True, sort=False)
        else:
            df = all_df.iloc[:0]
        if self.use_otm_iv:
            df['combined_iv'] = df['otm_iv']
        today_max_ts = df['ts'].unique().max()
        spot_today_max_ts = df['spot'][df['ts'] == today_max_ts].unique().tolist()

        logger.info(f'\n {symbol} {expiry} today max ts is {today_max_ts}\t type is {type(today_max_ts)}')
        logger.info(f'\n {symbol} {expiry} spot today max ts is {spot_today_max_ts}\t type is {type(spot_today_max_ts)}')
        list_exp = read_symbols['expiry'][read_symbols['symbol'] == 'NIFTY'].tolist()
        logger.info(f'\n {symbol} {expiry} list_exp is {list_exp}')

        if symbol == 'NIFTY' or symbol == 'FINNIFTY':
            if pd.to_datetime(expiry) == pd.to_datetime(list_exp[-1]):
                self.difference = 100
            else:
                self.difference = 50
        elif symbol == 'BANKNIFTY':
            self.difference = 100
        elif symbol == 'MIDCPNIFTY':
            self.difference = 25

        rounded_spot = round_spot(symbol=symbol, spot_multiple=self.difference, spot=spot_today_max_ts[0])
        logger.info(f"\n{symbol} {expiry} rounded spot is {rounded_spot} \t type is {type(rounded_spot)}")

        self.list_dict.append({symbol: [expiry, self.difference]})
        if len(self.list_dict) == 10:
            logger.info(f'\n list_dict of all s is \n{self.list_dict}')

        req = self._straddle_response(df, raw=True, count=st_cnt, interval=30, spot = rounded_spot, diff = self.difference, sym=symbol, exp = str(expiry), strd_clst = True)

        if req is not None:
            req.sort_values(['ts', 'strike'], inplace=True)
        else:
            # req = pd.DataFrame(columns=req1.columns)
            req = pd.DataFrame(columns=req.columns)

        req = req.replace({np.NAN: None}).round(2)
        strike_iv = req.groupby(['strike'], as_index=False).agg({'combined_iv': list, 'ts': list})
        strike_iv.sort_values(['strike'], inplace=True)
        strikes = strike_iv['strike'].tolist()
        max_len = max(len(x) for x in strike_iv['ts'])  # chk if len(strikes) == len(strikes_iv['ts'])
        strike_iv['combined_iv'] = strike_iv['combined_iv'].apply(lambda x: x + [None] * (max_len - len(x)))
        strike_iv['ts'] = strike_iv['ts'].apply(lambda x: x + [None] * (max_len - len(x)))

        # -----
        combined_iv_list = strike_iv['combined_iv'].tolist()
        for i in range(len(combined_iv_list[0])): #interpolation
            for j in range(len(strikes)):
                if combined_iv_list[j][i] is None:
                    lesser_iv = None
                    greater_iv = None
                    # Finding the lesser strike IV
                    for k in range(j - 1, -1, -1):
                        if combined_iv_list[k][i] is not None:
                            lesser_iv = combined_iv_list[k][i]
                            break
                    # Finding the greater strike IV
                    for k in range(j + 1, len(strikes)):
                        if combined_iv_list[k][i] is not None:
                            greater_iv = combined_iv_list[k][i]
                            break
                    if lesser_iv is not None and greater_iv is not None:
                        combined_iv_list[j][i] = (lesser_iv + greater_iv) / 2

        # iv = list(zip_longest(*combined_iv_list, fillvalue=None))
        # ts = list(zip_longest(*strike_iv['ts'].tolist(), fillvalue=None))
        iv = list(zip(*combined_iv_list))
        ts = list(zip(*strike_iv['ts'].tolist()))
        ts = [list(filter(lambda x: x is not None, t)) for t in ts]

        a = {'strikes': strikes, 'iv': iv, 'ts': ts, 'spot': [rounded_spot]}
        # logger.info(f"\n{symbol} {expiry} strd response is \n{a}")

        return {'strikes': strikes, 'iv': iv, 'ts': ts, 'spot': [rounded_spot], 'symbol': [symbol], 'expiry': [expiry]}

    def _straddle_response(self, df: pd.DataFrame, raw=False, count: int = None, interval: int = None, spot: int = None, diff: int = None, sym: str=None, exp:str=None, strd_clst:bool=False):
        count = 10 if count is None else count
        l_st, u_st = count + 1, count
        # logger.info(f'\ndf spot is \n {df["spot"]}')
        df['spot'] = pd.to_numeric(df['spot'], errors='coerce')
        df = df.dropna(subset=['spot'])
        if spot is not None:
            mean = spot
        else:
            mean = df['spot'].mean()
        # # logger.info(f'\nmean is {mean}')
        uq_strikes = pd.to_numeric(df['strike'], errors='coerce').dropna().unique()
        # uq_strikes = df['strike'].unique()
        uq_strikes.sort()
        # strikes = uq_strikes[uq_strikes <= mean][-l_st:].tolist() + uq_strikes[uq_strikes > mean][:u_st].tolist()
        if diff:
            up_strike = [spot + i * diff for i in range(15)]
            down_strike = [spot - i * diff for i in range(15)]
            down_strike.pop(0)
            strikes = up_strike + down_strike
            strikes = sorted(strikes)
        else:
            strikes = uq_strikes[uq_strikes <= mean][-l_st:].tolist() + uq_strikes[uq_strikes > mean][:u_st].tolist()

        # if strd_clst:
        #     logger.info(f'\nstrd_clst {sym} {exp} spot is {spot}\n uq_strikes are \n{uq_strikes}, \n strikes are \n{strikes}')
        # else:
        #     logger.info(f'\nconti_strd {sym} {exp} spot is {spot}\n uq_strikes are \n{uq_strikes}, \n strikes are \n{strikes}')

        df: pd.DataFrame = df[df['strike'].isin(strikes)].copy()
        df.drop(columns=['spot', 'range'], errors='ignore', inplace=True)
        df.sort_values(['ts', 'strike'], inplace=True)
        if interval and len(df):
            valid_ts = pd.date_range(start=df['ts'].min(), end=df['ts'].max(), freq=f'{interval}min')
            if len(valid_ts):
                df = df[df['ts'].isin(valid_ts)].copy()
        if raw:
            return df
        return self.df_response(df, to_millis=['ts'])

    @staticmethod
    def df_response(df: pd.DataFrame, to_millis: list = None) -> list[dict]:
        df = df.replace({np.NAN: None}).round(2)
        if to_millis is not None and len(to_millis) and len(df):
            for _col in to_millis:
                df[_col] = (df[_col].dt.tz_localize(IST).astype('int64') // 10**9) * 1000
        return df.to_dict('records')


service = ServiceApp()
app = service.app


if __name__ == '__main__':
    uvicorn.run('app:app', host='0.0.0.0', port=8851, workers=5)
