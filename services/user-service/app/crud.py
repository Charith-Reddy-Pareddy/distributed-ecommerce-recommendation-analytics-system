from sqlalchemy.orm import Session

from . import models, schemas


def create_user(db: Session, user: schemas.UserCreate) -> models.User:
    db_user = models.User(**user.model_dump())
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user


def get_user(db: Session, user_id: int) -> models.User | None:
    return db.query(models.User).filter(models.User.id == user_id).first()


def list_users(
    db: Session, skip: int = 0, limit: int = 100, country: str | None = None
) -> list[models.User]:
    query = db.query(models.User)
    if country:
        query = query.filter(models.User.country == country)
    return query.offset(skip).limit(limit).all()
