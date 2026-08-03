from sqlalchemy.orm import Session

from . import models, schemas


def create_product(db: Session, product: schemas.ProductCreate) -> models.Product:
    db_product = models.Product(**product.model_dump())
    db.add(db_product)
    db.commit()
    db.refresh(db_product)
    return db_product


def get_product(db: Session, product_id: int) -> models.Product | None:
    return db.query(models.Product).filter(models.Product.id == product_id).first()


def list_products(db: Session, skip: int = 0, limit: int = 100) -> list[models.Product]:
    return db.query(models.Product).offset(skip).limit(limit).all()


def get_products_by_ids(db: Session, product_ids: list[int]) -> list[models.Product]:
    return db.query(models.Product).filter(models.Product.id.in_(product_ids)).all()
