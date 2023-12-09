from datetime import date

import numpy as np
import pandas as pd
import uvicorn
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

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
        self.app.add_api_route('/symbol', methods=['GET'], endpoint=self.get_symbols)
        self.app.add_api_route('/straddle/minima', methods=['GET'], endpoint=self.fetch_straddle_minima)
        self.app.add_api_route('/straddle/iv', methods=['GET'], endpoint=self.fetch_straddle_iv)

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
        return self.df_response(df)

    def fetch_straddle_iv(self, symbol: str = Query(), expiry: date = Query()):
        df = DBHandler.get_straddle_iv_data(symbol, expiry)
        return self.df_response(df)

    @staticmethod
    def df_response(df: pd.DataFrame) -> list[dict]:
        df = df.replace({np.NAN: None}).round(2)
        return df.to_dict('records')


if __name__ == '__main__':
    service = ServiceApp()
    uvicorn.run(service.app, host='0.0.0.0', port=8501)
