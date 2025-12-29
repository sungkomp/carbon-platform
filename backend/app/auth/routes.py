from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.db import get_db
from app.auth.security import verify_password, create_token, get_current_user
from app.auth.models import User

router = APIRouter(prefix="/api/auth", tags=["auth"])

@router.post("/login")
def login(payload: dict, db: Session = Depends(get_db)):
    username = payload.get("username","")
    password = payload.get("password","")
    u = db.query(User).filter(User.username == username).one_or_none()
    if not u or not verify_password(password, u.password_hash):
        raise HTTPException(401, "Invalid credentials")
    return {"token": create_token(u), "roles": u.roles, "username": u.username}

@router.get("/me")
def me(user: User = Depends(get_current_user)):
    return {"username": user.username, "roles": user.roles}
