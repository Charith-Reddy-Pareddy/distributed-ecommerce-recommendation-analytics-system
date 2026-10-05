from fastapi import FastAPI

app = FastAPI(title="E-commerce Recommendation Analytics")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
