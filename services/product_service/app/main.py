from fastapi import FastAPI

app = FastAPI(title="Product Catalog Service")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "product-service"}
