from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class GeoPoint(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["Point"] = "Point"
    coordinates: tuple[float, float]

    @field_validator("coordinates")
    @classmethod
    def check_coordinates(cls, point: tuple[float, float]) -> tuple[float, float]:
        longitude, latitude = point
        if not -180 <= longitude <= 180:
            raise ValueError("longitude must be between -180 and 180")
        if not -90 <= latitude <= 90:
            raise ValueError("latitude must be between -90 and 90")
        return point


class Rating(BaseModel):
    model_config = ConfigDict(extra="forbid")

    average: float = Field(default=0, ge=0, le=5)
    count: int = Field(default=0, ge=0)


class ProductBase(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    name: str = Field(min_length=1, max_length=200)
    category: str = Field(min_length=1, max_length=100)
    price: Decimal = Field(ge=0, max_digits=10, decimal_places=2)
    currency: str = Field(default="USD", pattern=r"^[A-Z]{3}$")
    description: str = ""
    tags: list[str] = Field(default_factory=list)
    specifications: dict[str, str] = Field(default_factory=dict)
    images: list[str] = Field(default_factory=list)
    location: GeoPoint | None = None
    rating: Rating = Field(default_factory=Rating)
    asin: str | None = None


class ProductCreate(ProductBase):
    pass


class ProductOut(ProductBase):
    id: int = Field(gt=0)
