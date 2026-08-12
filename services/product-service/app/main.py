from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from . import crud, schemas
from .search_client import ensure_index, index_product, search_products


@asynccontextmanager
async def lifespan(app: FastAPI):
    await ensure_index()
    # ES has no persistent volume, so every restart needs repopulating
    # from MongoDB, the actual source of truth.
    products = await crud.list_products(skip=0, limit=10_000)
    for product in products:
        await index_product(product)
    yield


app = FastAPI(title="Product Catalog Service", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok", "service": "product-service"}


@app.post("/products", response_model=schemas.ProductOut, status_code=201)
async def create_product(product: schemas.ProductCreate):
    return await crud.create_product(product)


# Literal-path route must be registered before "/products/{product_id}"
# below, or FastAPI will match "/products/search" as product_id="search".
@app.get("/products/search")
async def search(
    q: str | None = None,
    category: str | None = None,
    brand: str | None = None,
    price_min: float | None = None,
    price_max: float | None = None,
    rating_min: float | None = None,
    lat: float | None = None,
    lon: float | None = None,
    radius_km: float | None = None,
    sort: str | None = None,
    limit: int = 20,
    offset: int = 0,
):
    search_result = await search_products(
        query=q,
        category=category,
        brand=brand,
        price_min=price_min,
        price_max=price_max,
        rating_min=rating_min,
        lat=lat,
        lon=lon,
        radius_km=radius_km,
        sort=sort,
        limit=limit,
        offset=offset,
    )
    return {
        "query": q,
        "category": category,
        "results": search_result["results"],
        "total": search_result["total"],
        "offset": offset,
        "limit": limit,
    }


@app.get("/products", response_model=list[schemas.ProductOut])
async def list_products(skip: int = 0, limit: int = 100):
    return await crud.list_products(skip, limit)


@app.get("/products/{product_id}", response_model=schemas.ProductOut)
async def get_product(product_id: int):
    product = await crud.get_product(product_id)
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    return product


@app.post("/products/batch", response_model=list[schemas.ProductOut])
async def get_products_batch(product_ids: list[int]):
    return await crud.get_products_by_ids(product_ids)
