from pydantic import BaseModel, Field


class GeoPoint(BaseModel):
    """GeoJSON Point -- [longitude, latitude], the shape both MongoDB and
    Elasticsearch expect for geo queries."""

    type: str = "Point"
    coordinates: tuple[float, float]


class Rating(BaseModel):
    average: float = 0.0
    count: int = 0


class ProductBase(BaseModel):
    name: str
    category: str
    price: float
    description: str = ""
    # Nested, variable-shape data -- natural in MongoDB, awkward in SQL.
    tags: list[str] = Field(default_factory=list)
    specifications: dict[str, str] = Field(default_factory=dict)
    images: list[str] = Field(default_factory=list)
    location: GeoPoint | None = None
    rating: Rating = Field(default_factory=Rating)
    # Real ASIN when sourced from Amazon data -- lets the dashboard link
    # to the actual product page.
    asin: str | None = None


class ProductCreate(ProductBase):
    pass


class ProductOut(ProductBase):
    id: int
