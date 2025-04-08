# api/routers/auth.py

"""
Router para gestionar la autenticación de usuarios.
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
import jwt
from datetime import datetime, timedelta
import os

from mom_server.db.user_repository import get_user, create_user, verify_password, get_all_users

router = APIRouter(tags=["Authentication"], prefix="/auth")

# Modelo de datos para usuarios
class UserCreate(BaseModel):
    username: str
    password: str

# Modelo de datos para login
class UserLogin(BaseModel):
    username: str
    password: str

# Clave secreta para JWT
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-secret-key")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def verify_token(token: str):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise HTTPException(status_code=401, detail="Token inválido")
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Token inválido")
    return username

@router.post("/register")
def register_user(user: UserCreate):
    """
    Registra un nuevo usuario en el sistema.
    """
    if get_user(user.username):
        raise HTTPException(status_code=400, detail="El usuario ya existe")
    
    try:
        create_user(user.username, user.password)
        return {"message": "Usuario registrado"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al registrar usuario: {str(e)}")

@router.post("/login")
def login_user(user: UserLogin):
    """
    Autentica un usuario y devuelve un token JWT.
    """
    db_user = get_user(user.username)
    if not db_user:
        raise HTTPException(status_code=401, detail="Usuario no encontrado")
    
    if not verify_password(db_user["password"], user.password):
        raise HTTPException(status_code=401, detail="Contraseña incorrecta")
    
    access_token = create_access_token(
        data={"sub": user.username}
    )
    return {"token": access_token}

@router.get("/users")
def get_users():
    """
    Devuelve la lista de todos los usuarios registrados.
    """
    return {"users": get_all_users()}
