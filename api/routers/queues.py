# api/routers/queues.py

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from api.routers.auth import verify_token
from mom_server.services.state import (
    get_queues,
    create_queue,
    delete_queue
)
from mom_server.services.messaging import (
    replicate_queue_to_cluster,
    replicate_queue_deletion_to_cluster
)

router = APIRouter()

class QueueData(BaseModel):
    name: str
    owner: str

@router.post("/")
def create_queue_endpoint(queue: QueueData, token: str):
    user = verify_token(token)
    queues = get_queues()
    if queue.name in queues:
        raise HTTPException(status_code=400, detail="Cola ya existe")
    try:
        create_queue(queue.name, user)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al crear la cola: {str(e)}")
    replicate_queue_to_cluster(queue.name, user)
    return {"message": f"Cola {queue.name} creada"}

@router.delete("/{queue_name}")
def delete_queue_endpoint(queue_name: str, token: str):
    user = verify_token(token)
    queues = get_queues()
    if queue_name not in queues:
        raise HTTPException(status_code=404, detail="Cola no encontrada")
    if queues[queue_name]["owner"] != user:
        raise HTTPException(status_code=403, detail="No autorizado para eliminar esta cola")
    try:
        delete_queue(queue_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al eliminar la cola: {str(e)}")
    replicate_queue_deletion_to_cluster(queue_name, user)
    return {"message": f"Cola {queue_name} eliminada"}

@router.get("/")
def list_queues_endpoint():
    queues = get_queues()
    return {"queues": list(queues.keys())}
