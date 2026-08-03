from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict

EventType = Literal["view", "add_to_cart", "purchase"]


class EventCreate(BaseModel):
    user_id: int
    product_id: int
    event_type: EventType


class EventOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    user_id: int
    product_id: int
    event_type: str
    created_at: datetime
