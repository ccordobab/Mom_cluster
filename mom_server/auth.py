# mom_server/auth.py
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
import jwt
import datetime
from mom_server.config import SECRET_KEY, ACCESS_TOKEN_EXPIRE_MINUTES

router = APIRouter()

# Simulación de base de datos en memoria
users = {}

# Modelos para autenticación
class User(BaseModel):
    username: str
    password: str

# Registro de usuario
@router.post("/register")
def register(user: User):
    if user.username in users:
        raise HTTPException(status_code=400, detail="Usuario ya existe")
    users[user.username] = user.password
    return {"message": "Usuario registrado"}

# Login y generación de token JWT
@router.post("/login")
def login(user: User):
    if users.get(user.username) != user.password:
        raise HTTPException(status_code=401, detail="Credenciales incorrectas")
    
    expiration = datetime.datetime.utcnow() + datetime.timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    token = jwt.encode({"username": user.username, "exp": expiration}, SECRET_KEY, algorithm="HS256")
    
    return {"token": token}

# Middleware para verificar tokens JWT
def verify_token(token: str):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        return payload["username"]
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expirado")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Token inválido")
