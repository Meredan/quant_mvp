# market_data/api.py
from ninja import Router
from .services import fetch_ticker_data

router = Router()

@router.get("/fetch/{symbol}")
async def get_market_data(request, symbol: str):
    data = await fetch_ticker_data(symbol)
    return {"symbol": symbol, "count": len(data), "sample": data[:1]}