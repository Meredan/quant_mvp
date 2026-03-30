from datetime import datetime
from pydantic import BaseModel, ConfigDict

class MarketData(BaseModel):
    model_config = ConfigDict(strict=True)

    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
