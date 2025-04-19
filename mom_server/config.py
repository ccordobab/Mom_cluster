import os
import json
import logging
from dotenv import load_dotenv

# Código existente se mantiene
env_file = os.getenv("ENV_FILE", ".env.node1")
load_dotenv(dotenv_path=env_file)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Mapeo de puertos API a puertos GRPC
PORT_MAPPING = {
    "8000": "50051",
    "8001": "50052",
    "8002": "50053",
    "8003": "50054",
    "8004": "50055"
}

# Intentar cargar configuración del clúster desde JSON
CLUSTER_CONFIG = {}
try:
    with open('cluster_config.json', 'r') as f:
        CLUSTER_CONFIG = json.load(f)
    logger.info(f"Configuración cargada desde cluster_config.json: {CLUSTER_CONFIG}")
except (FileNotFoundError, json.JSONDecodeError) as e:
    logger.warning(f"No se pudo cargar la configuración desde archivo: {e}")

# Parámetros de autenticación y otros
SECRET_KEY = os.getenv("SECRET_KEY", "supersecreto")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", 60))
DATABASE_URL = os.getenv("DATABASE_URL", "mysql://user:password@localhost/mom_db")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# Obtener nodos del clúster desde la variable de entorno CLUSTER_NODES (separados por comas)
CLUSTER_NODES_STR = os.getenv("CLUSTER_NODES", "")
if CLUSTER_NODES_STR:
    CLUSTER_NODES = [node.strip() for node in CLUSTER_NODES_STR.split(",") if node.strip()]
    logger.info(f"Nodos del clúster desde env: {CLUSTER_NODES}")
else:
    CLUSTER_NODES = ["localhost:8000", "localhost:8001", "localhost:8002"]
    logger.warning(f"No se encontró configuración de nodos, usando valores por defecto: {CLUSTER_NODES}")

GRPC_PORT = os.getenv("GRPC_PORT", "50051")
logger.info(f"Puerto GRPC local: {GRPC_PORT}")

NODE_MAP = {}
if CLUSTER_CONFIG:
    for key, node_data in CLUSTER_CONFIG.items():
        api_port = node_data.get('api_port')
        grpc_port_val = node_data.get('grpc_port')
        if api_port and grpc_port_val:
            NODE_MAP[f"localhost:{api_port}"] = f"localhost:{grpc_port_val}"
logger.info(f"NODE_MAP: {NODE_MAP}")

SELF_HOST = os.getenv("SELF_HOST", "localhost:8000")
logger.info(f"SELF_HOST: {SELF_HOST}")

# NUEVAS CONFIGURACIONES PARA PARTICIONAMIENTO
PARTITIONING_ENABLED = os.getenv("PARTITIONING_ENABLED", "true").lower() == "true"
PARTITION_REPLICATION_FACTOR = int(os.getenv("PARTITION_REPLICATION_FACTOR", "2"))
logger.info(f"Particionamiento habilitado: {PARTITIONING_ENABLED}")
logger.info(f"Factor de replicación: {PARTITION_REPLICATION_FACTOR}")

def api_to_grpc_address(api_address):
    if not api_address:
        return None
    if api_address in NODE_MAP:
        return NODE_MAP[api_address]
    parts = api_address.split(":")
    if len(parts) != 2:
        logger.warning(f"Formato de dirección inválido: {api_address}")
        return None
    host, port = parts
    grpc_port = PORT_MAPPING.get(port, str(int(port) + 42051))
    return f"{host}:{grpc_port}"

def get_config():
    return {
        "cluster_nodes": CLUSTER_NODES,
        "grpc_port": GRPC_PORT,
        "node_index": int(os.getenv("NODE_INDEX", "0")),
        "port_mapping": PORT_MAPPING,
        "node_map": NODE_MAP,
        "self_host": SELF_HOST,
        "secret_key": SECRET_KEY,
        "access_token_expire_minutes": ACCESS_TOKEN_EXPIRE_MINUTES,
        "database_url": DATABASE_URL,
        "redis_url": REDIS_URL,
        # Nuevos campos de configuración
        "partitioning_enabled": PARTITIONING_ENABLED,
        "partition_replication_factor": PARTITION_REPLICATION_FACTOR
    }