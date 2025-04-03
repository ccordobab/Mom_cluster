# mom_server/config.py
import os
from dotenv import load_dotenv

# Cargar variables de entorno desde un archivo .env si existe
load_dotenv()

# Clave secreta para JWT
SECRET_KEY = os.getenv("SECRET_KEY", "supersecreto")

# Configuración de la base de datos (usando MariaDB o Redis)
DATABASE_URL = os.getenv("DATABASE_URL", "mysql://user:password@localhost/mom_db")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# Configuración del clúster
CLUSTER_NODES = os.getenv("CLUSTER_NODES", "node1:8000,node2:8001").split(",")

# Configuración de seguridad (tiempo de expiración del token JWT)
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", 60))
