from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field


class ProductCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    name: str = Field(min_length=1, max_length=200)
    category: str = Field(min_length=1, max_length=100)
    price: Decimal = Field(ge=Decimal("0"), max_digits=10, decimal_places=2)
    currency: str = Field(default="USD", pattern=r"^[A-Z]{3}$")
