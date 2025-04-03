# mom_server/cluster.py
from fastapi import APIRouter
from mom_server.config import CLUSTER_NODES

router = APIRouter()

# Obtener los nodos del clúster
@router.get("/nodes")
def get_nodes():
    return {"nodes": CLUSTER_NODES}
