from fastapi import FastAPI, HTTPException
import talib
import numpy as np
import yfinance as yf
import pandas as pd

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Sloan Analyst API - Hello World!"}

@app.get("/test_talib")
async def test_talib():
    # --- Data Retrieval ---
    try:
        data = yf.download("META", period="1y", progress=False)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error downloading data from yfinance: {str(e)}")

    if data is None or data.empty:
        raise HTTPException(status_code=500, detail="Error: yfinance returned an empty DataFrame.")

    # --- Data Validation ---
    if "Close" not in data.columns:
        raise HTTPException(status_code=500, detail="Error: No 'Close' column found in yfinance response.")

    # --- Data Cleaning ---
    close_prices = data["Close"].dropna()  # Remove NaNs

    if close_prices.shape[0] < 20:
        raise HTTPException(status_code=400, detail="Error: Not enough data points for SMA calculation (minimum 20 required).")

    # Convert to a 1D NumPy array (force flattening in case of shape issues)
    close_prices = np.asarray(close_prices, dtype=np.float64).flatten()

    # --- TA-Lib Calculation ---
    try:
        sma = talib.SMA(close_prices, timeperiod=20)

        if np.isnan(sma[-1]):  # Ensure SMA has valid values
            raise ValueError("SMA calculation resulted in NaN.")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error calculating SMA with TA-Lib: {str(e)}")

    return {"sma": float(sma[-1])}  # Convert NumPy float to standard Python float for JSON serialization
