from datetime import date

import numpy as np
import pandas as pd
import uvicorn
from fastapi import FastAPI, Query, status
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

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

    def add_routes(self):
        self.app.add_api_route('/', methods=['GET'], endpoint=self.default)
        self.app.add_api_route('/symbol', methods=['GET'], endpoint=self.get_symbols)
        self.app.add_api_route('/straddle/minima', methods=['GET'], endpoint=self.fetch_straddle_minima)
        self.app.add_api_route('/straddle/iv', methods=['GET'], endpoint=self.fetch_straddle_iv)
        self.app.add_api_route('/straddle/cluster', methods=['GET'], endpoint=self.fetch_straddle_cluster)

    @staticmethod
    def default():
        body = {'status': 'success', 'message': '', 'data': [], 'response_code': None}
        return JSONResponse(content=jsonable_encoder(body), status_code=status.HTTP_200_OK)

    def get_symbols(self):
        if self.symbol_expiry_map is None:
            ins_df, tokens, token_xref = get_req_contracts()
            ins_df['expiry'] = ins_df['expiry'].dt.strftime('%Y-%m-%d')
            agg = ins_df[ins_df['instrument_type'].isin(['CE', 'PE'])].groupby(['name'], as_index=False).agg({'expiry': set, 'tradingsymbol': 'count'})
            agg['expiry'] = agg['expiry'].apply(list)
            self.symbol_expiry_map = agg.to_dict('records')
        return self.symbol_expiry_map

    def fetch_straddle_minima(self, symbol: str = Query(), expiry: date = Query()):
        df = DBHandler.get_straddle_minima(symbol, expiry)
        return self._straddle_response(df)

    def fetch_straddle_iv(self, symbol: str = Query(), expiry: date = Query()):
        df = DBHandler.get_straddle_iv_data(symbol, expiry)
        return self._straddle_response(df)

    def fetch_straddle_cluster(self, symbol: str = Query(), expiry: date = Query(), interval: str = Query('5min')):
        df = DBHandler.get_straddle_iv_data(symbol, expiry)
        allowed = pd.date_range(df['ts'].min(), df['ts'].max(), freq=interval)
        req = df[df['ts'].isin(allowed)].copy()
        req = self._straddle_response(req, raw=True)
        req = req.replace({np.NAN: None}).round(2)
        strike_iv = req.groupby(['strike'], as_index=False).agg({'combined_iv': list})
        strike_iv.sort_values(['strike'], inplace=True)
        strikes = strike_iv['strike'].tolist()
        iv = list(zip(*strike_iv['combined_iv'].tolist()))
        return {'strikes': strikes, 'iv': iv}

    def _straddle_response(self, df: pd.DataFrame, raw=False):
        # df['range'] = (df['spot'] - df['strike']).abs() < (df['spot'] * 0.05)
        # strikes = df[df['range']]['strike'].unique()
        mean = df['spot'].mean()
        uq_strikes = df['strike'].unique()
        uq_strikes.sort()
        strikes = uq_strikes[uq_strikes <= mean][-11:].tolist() + uq_strikes[uq_strikes > mean][:10].tolist()
        # print(uq_strikes, strikes)
        df = df[df['strike'].isin(strikes)].copy()
        df.drop(columns=['spot', 'range'], errors='ignore', inplace=True)
        df.sort_values(['ts', 'strike'], inplace=True)
        if raw:
            return df
        return self.df_response(df, to_millis=['ts'])

    @staticmethod
    def df_response(df: pd.DataFrame, to_millis: list = None) -> list[dict]:
        df = df.replace({np.NAN: None}).round(2)
        if to_millis is not None and len(to_millis):
            for _col in to_millis:
                df[_col] = (df[_col].astype('int64') // 10**9) * 1000
        return df.to_dict('records')


service = ServiceApp()
app = service.app


if __name__ == '__main__':
    uvicorn.run('app:app', host='0.0.0.0', port=8501, workers=2)
