from typing import Protocol

class MarketDataFetcher(Protocol):
    async def fetch_historical_data(self, ticker: str, limit: int = 10000) -> str:
        """
        Fetches historical data for a given ticker and returns the absolute
        path to the generated Parquet file.
        """
        ...
