from fastapi import FastAPI
from fastapi.responses import JSONResponse
import pandas as pd
import numpy as np
import uvicorn
from pydantic import BaseModel


class Search(BaseModel):
    UnderlyingInstrumentId: int = False
    UnderlyingIndexName: str = False
    ContractExpiration: str
    StrikePrice: int


app = FastAPI()

contracts_df = None


@app.post('/')
def csv_search(request: Search):
    """
    read csv & search with the parameters
    """

    global contracts_df
    if contracts_df is None:
        try:
            contracts_df = pd.read_csv("entity_data/Instruments_xts.csv", low_memory=False)
        except:
            return {"message": "csv file not found", "status": "fail", "contracts": []}

    query = f"ContractExpiration.str.startswith('{request.ContractExpiration.upper()}').values & StrikePrice == {request.StrikePrice}"

    # searching with UnderlyingInstrumentId
    if request.UnderlyingInstrumentId:
        query = f"UnderlyingInstrumentId == {request.UnderlyingInstrumentId} & " + query

    # searching with UnderlyingIndexName
    if request.UnderlyingIndexName:
        query = f"Name == '{request.UnderlyingIndexName.upper()}' & " + query

    if (not request.UnderlyingInstrumentId) and (not request.UnderlyingIndexName):
        return {"message": "Invalid inputs - UnderlyingInstrumentId or UnderlyingIndexName not provided", "status": "fail", "contracts": []}

    contracts_filter = contracts_df.query(query)    # search in contracts

    if contracts_filter.empty:
        return {"message": "Data not found. Please check the details again", "status": "fail", "contracts": []}

    contracts_filter = contracts_filter.fillna("")    # handle NaN

    return {"message": "", "status": "success", "contracts": contracts_filter.to_dict("records")}


if __name__ == '__main__':
    port = 8755
    print(f"http://127.0.0.1:{port}")
    uvicorn.run("xts_search_api:app", host='0.0.0.0', port=port)
