import polars as pl
from opentelemetry import trace

tracer = trace.get_tracer(__name__)

class MarketAnalyzer:
    @staticmethod
    def calculate_indicators(lf: pl.LazyFrame) -> pl.DataFrame:
        """
        Calculates SMA (14) and RSI (14) on a Polars LazyFrame.
        LazyFrames save memory by optimizing the execution graph before running.
        """
        with tracer.start_as_current_span("calculate_polars_indicators"):
            lf = lf.with_columns([
                pl.col("close").rolling_mean(window_size=14).alias("sma_14"),
                pl.col("close").diff().alias("price_diff")
            ])
            
            lf = lf.with_columns([
                pl.when(pl.col("price_diff") > 0).then(pl.col("price_diff")).otherwise(0.0).alias("gain"),
                pl.when(pl.col("price_diff") < 0).then(pl.col("price_diff").abs()).otherwise(0.0).alias("loss"),
            ])
            
            lf = lf.with_columns([
                pl.col("gain").rolling_mean(window_size=14).alias("avg_gain"),
                pl.col("loss").rolling_mean(window_size=14).alias("avg_loss"),
            ])
            
            lf = lf.with_columns([
                pl.when(pl.col("avg_loss") == 0)
                .then(float("inf"))
                .otherwise(pl.col("avg_gain") / pl.col("avg_loss"))
                .alias("rs")
            ])
            
            lf = lf.with_columns([
                pl.when(pl.col("avg_loss") == 0)
                .then(100.0)
                .otherwise(100.0 - (100.0 / (1.0 + pl.col("rs"))))
                .alias("rsi_14")
            ])
            
            # Execute the optimized graph
            return lf.select([
                "timestamp", "open", "high", "low", "close", "volume", "sma_14", "rsi_14"
            ]).collect()
