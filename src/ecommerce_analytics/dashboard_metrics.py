"""Response models for real-data dashboard charts."""

from datetime import date
from enum import Enum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from ecommerce_analytics.schemas import EventType


class EventSource(str, Enum):
    APPLICATION = "application_events"
    RETAILROCKET = "retailrocket"


class EventVolumePoint(BaseModel):
    model_config = ConfigDict(extra="forbid")

    day: date
    event_type: EventType
    count: int = Field(strict=True, ge=0)


class EventVolumeSeries(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: EventSource
    points: list[EventVolumePoint]


class ProductInteraction(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    source_product_id: str = Field(min_length=1)
    label: str = Field(min_length=1)
    views: int = Field(strict=True, ge=0)
    add_to_carts: int = Field(strict=True, ge=0)
    purchases: int = Field(strict=True, ge=0)


class ProductInteractionSeries(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: EventSource
    products: list[ProductInteraction]


class RatingBucket(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rating: float = Field(ge=1, le=5)
    count: int = Field(strict=True, ge=0)


class AmazonRatingDistribution(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: Literal["amazon_reviews_2023"] = "amazon_reviews_2023"
    asin: str | None = Field(default=None, pattern=r"^[A-Z0-9]{10}$")
    buckets: list[RatingBucket]
