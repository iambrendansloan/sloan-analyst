from fastapi import FastAPI, HTTPException
import yfinance as yf
import talib
import pandas as pd
from database import insert_historical_data, get_historical_data_db

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Sloan Analyst API - Hello World!"}

@app.post("/api/meta/fetch/{period}")
async def fetch_and_store_data(period: str):
    """
    Fetches historical data for META from yfinance for the given period
    and stores it in the database.
    """
    valid_periods = ["1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max"]
    if period not in valid_periods:
        raise HTTPException(status_code=400, detail="Invalid period")

    try:
        data = yf.download("META", period=period, progress=False)
        if data.empty:
            raise HTTPException(status_code=404, detail="No data found from yfinance")

        # Rename columns to lowercase to match database
        data.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"}, inplace=True)
        success = insert_historical_data(data)  # Store in database
        if success:
            return {"message": f"Data for {period} fetched and stored successfully."}
        else:
            raise HTTPException(status_code=500, detail="Failed to store data in database")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/meta/historical/{period}")
async def read_historical_data(period: str):
    """
    Retrieves historical data for META from the database for the given period.
    """
    data = get_historical_data_db(period)
    if data is None:
        raise HTTPException(status_code=404, detail="No data found for the given period")
    return data.to_dict(orient="index")  # Return as JSON

@app.get("/api/meta/sma/{period}/{timeperiod}")
async def calculate_sma(period: str, timeperiod: int):
    """Calculates SMA for META using data from the database."""
    data = get_historical_data_db(period)
    if data is None:
        raise HTTPException(status_code=404, detail="No data found for the given period")
    if len(data) < timeperiod:
        raise HTTPException(status_code=400, detail="Not enough data for the given timeperiod")

    sma = talib.SMA(data['close'], timeperiod=timeperiod)
    return {"sma": sma.to_dict()}  # Return as JSON

@app.get("/api/meta/ema/{period}/{timeperiod}")
async def calculate_ema(period: str, timeperiod: int):
    """Calculates EMA for META using data from the database."""
    data = get_historical_data_db(period)
    if data is None:
        raise HTTPException(status_code=404, detail="No data found for the given period")
    if len(data) < timeperiod:
        raise HTTPException(status_code=400, detail="Not enough data for the given timeperiod")

    ema = talib.EMA(data['close'], timeperiod=timeperiod)
    return {"ema": ema.to_dict()}

@app.get("/api/meta/rsi/{period}/{timeperiod}")
async def calculate_rsi(period: str, timeperiod: int):
    """Calculates RSI for META using data from the database."""
    data = get_historical_data_db(period)
    if data is None:
        raise HTTPException(status_code=404, detail="No data found for the given period")
    if len(data) < timeperiod:
        raise HTTPException(status_code=400, detail="Not enough data for the given timeperiod")

    rsi = talib.RSI(data['close'], timeperiod=timeperiod)
    return {"rsi": rsi.to_dict()}
