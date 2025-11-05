from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from src.models.call_sessions import CallSession
from src.schemas.call_sessions import CallSession as CallSessionModel
from src.db.database import get_db
from src.utils.response_wrapper import api_response

router = APIRouter()


@router.post("/call-sessions/")
def save_call_session(call_session: CallSession, db: Session = Depends(get_db)):    
    
    new_customer = CallSessionModel(
        **call_session.model_dump())
    db.add(new_customer)
    db.commit()
    db.refresh(new_customer)
    return api_response(data=new_customer, message="call session saved")