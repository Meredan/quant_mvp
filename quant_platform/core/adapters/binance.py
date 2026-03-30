import httpx
import polars as pl
import asyncio
from typing import List, Any
from pathlib import Path
from opentelemetry import trace
from ..ports.market_data import MarketDataFetcher

tracer = trace.get_tracer(__name__)

class BinanceFetcher(MarketDataFetcher):
    BASE_URL = "https://api.binance.com/api/v3/klines"

    async def _fetch_chunk(self, client: httpx.AsyncClient, ticker: str, end_time: int | None = None) -> List[Any]:
        params = {
            "symbol": ticker.upper(),
            "interval": "1m",
            "limit": 1000
        }
        if end_time:
            params["endTime"] = end_time
            
        response = await client.get(self.BASE_URL, params=params)
        response.raise_for_status()
        return response.json()

    async def fetch_historical_data(self, ticker: str, limit: int = 10000) -> str:
        with tracer.start_as_current_span("fetch_binance_data") as fetch_span:
            fetch_span.set_attribute("ticker", ticker)
            fetch_span.set_attribute("limit", limit)
            
            all_klines = []
            
            async with httpx.AsyncClient() as client:
                end_time = None
                remaining = limit
                
                while remaining > 0:
                    chunk = await self._fetch_chunk(client, ticker, end_time)
                    if not chunk:
                        break
                    
                    # Binance returns oldest to newest, so the earliest timestamp is at index 0
                    all_klines = chunk + all_klines
                    remaining -= len(chunk)
                    
                    # Next request should end right before the earliest timestamp in this chunk
                    end_time = chunk[0][0] - 1
                    
                    # Small delay to prevent hitting rate limits
                    await asyncio.sleep(0.05)

            if len(all_klines) > limit:
                all_klines = all_klines[-limit:]

        with tracer.start_as_current_span("write_parquet_binance") as write_span:
            # Schema based on Binance kline format
            schema = [
                "timestamp", "open", "high", "low", "close", "volume", 
                "close_time", "quote_asset_volume", "number_of_trades", 
                "taker_buy_base", "taker_buy_quote", "ignore"
            ]
            
            df = pl.DataFrame(all_klines, schema=schema, orient="row")
            
            # Extract and cast the specific columns needed by the Domain model
            df = df.select([
                pl.col("timestamp").cast(pl.Int64).cast(pl.Datetime(time_unit="ms")),
                pl.col("open").cast(pl.String).cast(pl.Float64),
                pl.col("high").cast(pl.String).cast(pl.Float64),
                pl.col("low").cast(pl.String).cast(pl.Float64),
                pl.col("close").cast(pl.String).cast(pl.Float64),
                pl.col("volume").cast(pl.String).cast(pl.Float64),
            ])
            
            output_dir = Path("/tmp/market_data")
            output_dir.mkdir(parents=True, exist_ok=True)
            
            file_path = output_dir / f"{ticker}_1m.parquet"
            # Write out to Parquet, columnar storage for high efficiency
            df.write_parquet(file_path)
            
            write_span.set_attribute("file_path", str(file_path))
            write_span.set_attribute("rows_written", len(df))
            
            return str(file_path)
