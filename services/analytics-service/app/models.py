from sqlalchemy import Column, Date, Integer, String, UniqueConstraint

from .database import Base


class ProductStats(Base):
    __tablename__ = "product_stats"

    product_id = Column(Integer, primary_key=True)
    views = Column(Integer, default=0, nullable=False)
    add_to_carts = Column(Integer, default=0, nullable=False)
    purchases = Column(Integer, default=0, nullable=False)


class DailyEventCount(Base):
    __tablename__ = "daily_event_counts"
    __table_args__ = (UniqueConstraint("day", "event_type", name="uq_day_event_type"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    day = Column(Date, nullable=False, index=True)
    event_type = Column(String(20), nullable=False, index=True)
    count = Column(Integer, default=0, nullable=False)
