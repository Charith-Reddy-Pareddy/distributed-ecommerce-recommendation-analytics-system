from contextlib import asynccontextmanager
import os
from typing import AsyncIterator

from elasticsearch import AsyncElasticsearch
from fastapi import Depends, FastAPI, HTTPException, Path, Query, Request
from pymongo import AsyncMongoClient

from services.product_service.app.catalog import MongoProductRepository, ProductRepository
from services.product_service.app.schemas import ProductCreate, ProductOut
from services.product_service.app.search import ElasticsearchProductIndex, ProductIndex


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    mongo = AsyncMongoClient(
        os.environ.get("MONGODB_URL", "mongodb://localhost:27017"),
        serverSelectionTimeoutMS=5000,
    )
    elasticsearch = AsyncElasticsearch(
        os.environ.get("ELASTICSEARCH_URL", "http://localhost:9200"),
        request_timeout=5,
    )
    app.state.product_repository = MongoProductRepository(
        mongo, os.environ.get("MONGODB_DATABASE", "product_catalog")
    )
    app.state.product_index = ElasticsearchProductIndex(
        elasticsearch, os.environ.get("ELASTICSEARCH_INDEX", "products")
    )
    try:
        yield
    finally:
        await elasticsearch.close()
        await mongo.close()


app = FastAPI(title="Product Catalog Service", lifespan=lifespan)


def get_product_repository(request: Request) -> ProductRepository:
    return request.app.state.product_repository


def get_product_index(request: Request) -> ProductIndex:
    return request.app.state.product_index


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "product-service"}


@app.post("/products", response_model=ProductOut, status_code=201)
async def create_product(
    product: ProductCreate,
    repository: ProductRepository = Depends(get_product_repository),
    product_index: ProductIndex = Depends(get_product_index),
) -> ProductOut:
    saved = await repository.create(product)
    await product_index.index_product(saved)
    return saved


@app.get("/products", response_model=list[ProductOut])
async def list_products(
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=500),
    repository: ProductRepository = Depends(get_product_repository),
) -> list[ProductOut]:
    return await repository.list(skip, limit)


@app.get("/products/search", response_model=list[ProductOut])
async def search_products(
    q: str = Query(min_length=1, max_length=200),
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=20, ge=1, le=100),
    product_index: ProductIndex = Depends(get_product_index),
) -> list[ProductOut]:
    return await product_index.search(q, skip, limit)


@app.get("/products/{product_id}", response_model=ProductOut)
async def get_product(
    product_id: int = Path(gt=0),
    repository: ProductRepository = Depends(get_product_repository),
) -> ProductOut:
    product = await repository.get(product_id)
    if product is None:
        raise HTTPException(status_code=404, detail="Product not found")
    return product
