from sqlalchemy import Column, Integer, String, Float, Text

from .database import Base


class Product(Base):
    __tablename__ = "products"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    category = Column(String(120), index=True, nullable=False)
    price = Column(Float, nullable=False)
    description = Column(Text, default="")
