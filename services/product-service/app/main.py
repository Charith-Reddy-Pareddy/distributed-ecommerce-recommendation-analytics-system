from fastapi import FastAPI, HTTPException

from . import crud, schemas

app = FastAPI(title="Product Catalog Service")


@app.get("/health")
def health():
    return {"status": "ok", "service": "product-service"}


@app.post("/products", response_model=schemas.ProductOut, status_code=201)
async def create_product(product: schemas.ProductCreate):
    return await crud.create_product(product)


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
