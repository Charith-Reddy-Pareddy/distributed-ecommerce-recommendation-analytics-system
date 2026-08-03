from pydantic import BaseModel, Field


class GeoPoint(BaseModel):
    """GeoJSON Point -- [longitude, latitude], matching both MongoDB's
    and Elasticsearch's geo field conventions, so the same coordinates
    seeded here work unchanged for the geo-filtered search added in
    Elasticsearch later.
    """

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
    # The "enriched" part of "enriched product information": nested,
    # variable-shape data that's awkward in a flat SQL row but natural
    # as a MongoDB document.
    tags: list[str] = Field(default_factory=list)
    specifications: dict[str, str] = Field(default_factory=dict)
    images: list[str] = Field(default_factory=list)
    location: GeoPoint | None = None
    rating: Rating = Field(default_factory=Rating)


class ProductCreate(ProductBase):
    pass


class ProductOut(ProductBase):
    id: int
