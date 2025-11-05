from datetime import date
from pydantic import BaseModel
from pydantic import ConfigDict

class CallSession(BaseModel):    
    customer_id: int
    period_date: date
    dropped_calls: int
    blocked_calls: int
    unanswered_calls: int 
    customer_care_calls: int 
    three_way_calls: int 
    received_calls: int 
    outbound_calls: int 
    inbound_calls: int 
    peak_calls_in_out: int 
    off_peak_calls_in_out: int 
    dropped_blocked_calls: int 
    call_forwarding_calls: int 
    call_waiting_calls: int 
    director_assisted_calls: int 
    

    model_config = ConfigDict(
        validate_by_name=True
    )
