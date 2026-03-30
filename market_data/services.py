import httpx
import asyncio
from datetime import datetime, timedelta

BINANCE_URL = "https://api.binance.com/api/v3/klines"

async def fetch_ticker_data(symbol: str, minutes: int = 10000):
    """
    Fetch historical kline data from Binance.
    In chunks of 1000 candles.
    """
    limit_per_request = 1000
    chunks = minutes // limit_per_request
    interval = "1m"
    all_data = []

    async with httpx.AsyncClient() as client:
        tasks = []
        for i in range(chunks):
            params = {
                "symbol": symbol.upper(),
                "interval": interval,
                "limit": limit_per_request,
            }
            tasks.append(client.get(BINANCE_URL, params=params))
        
        responses = await asyncio.gather(*tasks)
        for response in responses:
            if response.status_code == 200:
                all_data.extend(response.json())
                
    return all_data
