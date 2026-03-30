import polars as pl
from ninja import NinjaAPI
from typing import List, Dict, Any
from opentelemetry import trace

from quant_platform.core.adapters.binance import BinanceFetcher
from quant_platform.core.domain.indicators import MarketAnalyzer

api = NinjaAPI(title="Quant High-Performance API", version="1.0.0")
tracer = trace.get_tracer(__name__)

@api.get("/analyze/{ticker}")
async def analyze_ticker(request, ticker: str):
    """
    Triggers the high-performance pipeline:
    1. Fetch data from Binance (Async)
    2. Save to Parquet (Columnar Storage)
    3. Calculate SMA/RSI using Polars (LazyFrame optimization)
    4. Return results as JSON
    """
    with tracer.start_as_current_span("api_analyze_endpoint") as span:
        span.set_attribute("ticker", ticker)
        
        # 1. Fetching & Saving to Parquet
        fetcher = BinanceFetcher()
        parquet_path = await fetcher.fetch_historical_data(ticker, limit=10000)
        
        # 2. Polars Analysis
        # Scanning parquet creates a LazyFrame - no data is loaded yet
        lf = pl.scan_parquet(parquet_path)
        
        analyzer = MarketAnalyzer()
        results_df = analyzer.calculate_indicators(lf)
        
        # Convert the last 100 rows to list of dicts for return
        # In a real app we might stream this or just return summary stats
        payload = results_df.tail(100).to_dicts()
        
        return {
            "ticker": ticker,
            "count": len(results_df),
            "data": payload
        }
