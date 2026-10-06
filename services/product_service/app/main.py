from contextlib import asynccontextmanager
import os

from fastapi import Depends, FastAPI, HTTPException, Path, Query, Request
from pymongo import AsyncMongoClient

from services.product_service.app.catalog import MongoProductRepository, ProductRepository
from services.product_service.app.schemas import ProductCreate, ProductOut


@asynccontextmanager
async def lifespan(app: FastAPI):
    client = AsyncMongoClient(
        os.environ.get("MONGODB_URL", "mongodb://localhost:27017"),
        serverSelectionTimeoutMS=5000,
    )
    app.state.product_repository = MongoProductRepository(
        client, os.environ.get("MONGODB_DATABASE", "product_catalog")
    )
    try:
        yield
    finally:
        await client.close()


app = FastAPI(title="Product Catalog Service", lifespan=lifespan)


def get_product_repository(request: Request) -> ProductRepository:
    return request.app.state.product_repository


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "product-service"}


@app.post("/products", response_model=ProductOut, status_code=201)
async def create_product(
    product: ProductCreate,
    repository: ProductRepository = Depends(get_product_repository),
) -> ProductOut:
    return await repository.create(product)


@app.get("/products", response_model=list[ProductOut])
async def list_products(
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=500),
    repository: ProductRepository = Depends(get_product_repository),
) -> list[ProductOut]:
    return await repository.list(skip, limit)


@app.get("/products/{product_id}", response_model=ProductOut)
async def get_product(
    product_id: int = Path(gt=0),
    repository: ProductRepository = Depends(get_product_repository),
) -> ProductOut:
    product = await repository.get(product_id)
    if product is None:
        raise HTTPException(status_code=404, detail="Product not found")
    return product
