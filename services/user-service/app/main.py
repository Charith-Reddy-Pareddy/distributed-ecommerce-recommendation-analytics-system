from fastapi import Depends, FastAPI, HTTPException
from sqlalchemy.orm import Session

from . import crud, models, schemas
from .database import engine, get_db
from .metrics import MetricsMiddleware, metrics_response

models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="User Service")
app.add_middleware(MetricsMiddleware)


@app.get("/metrics")
def metrics():
    return metrics_response()


@app.get("/health")
def health():
    return {"status": "ok", "service": "user-service"}


@app.post("/users", response_model=schemas.UserOut, status_code=201)
def create_user(user: schemas.UserCreate, db: Session = Depends(get_db)):
    return crud.create_user(db, user)


@app.get("/users", response_model=list[schemas.UserOut])
def list_users(
    skip: int = 0,
    limit: int = 100,
    country: str | None = None,
    db: Session = Depends(get_db),
):
    return crud.list_users(db, skip, limit, country)


@app.get("/users/{user_id}", response_model=schemas.UserOut)
def get_user(user_id: int, db: Session = Depends(get_db)):
    user = crud.get_user(db, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user
