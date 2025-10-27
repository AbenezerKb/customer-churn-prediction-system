from sqlalchemy import Column, Integer, String, DateTime
from src.db.database import Base

class CallSession(Base):
    __tablename__ = "call_details"

    id = Column(Integer, primary_key=True, index=True)
    customer_id  = Column(Integer, nullable=False)
    period_date  = Column(DateTime, nullable=False)
    dropped_calls = Column(Integer, nullable=False)
    blocked_calls  = Column(Integer, nullable=False)
    unanswered_calls  = Column(Integer, nullable=False)
    customer_care_calls  = Column(Integer, nullable=False)
    three_way_calls  = Column(Integer, nullable=False)
    received_calls  = Column(Integer, nullable=False)
    outbound_calls  = Column(Integer, nullable=False)
    inbound_calls  = Column(Integer, nullable=False)
    peak_calls_in_out  = Column(Integer, nullable=False)
    off_peak_calls_in_out  = Column(Integer, nullable=False)
    dropped_blocked_calls  = Column(Integer, nullable=False)
    call_forwarding_calls  = Column(Integer, nullable=False)
    call_waiting_calls  = Column(Integer, nullable=False)
    director_assisted_calls  = Column(Integer, nullable=False)
  