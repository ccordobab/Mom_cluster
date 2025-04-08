# api/routers/topics.py

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

# Importamos la función verify_token de donde la tengas (ej: mom_server.auth o mom_server.services.auth)
from api.routers.auth import verify_token  

# Importamos las funciones de estado para tópicos
from mom_server.services.state import (
    get_topics,
    create_topic,
    delete_topic
)

# Importamos las funciones de replicación
from mom_server.services.messaging import (
    replicate_topic_to_cluster,
    replicate_topic_deletion_to_cluster
)

router = APIRouter()

class TopicQueue(BaseModel):
    name: str
    owner: str

@router.post("/")
def create_topic_endpoint(topic: TopicQueue, token: str):
    user = verify_token(token)
    topics = get_topics()
    if topic.name in topics:
        raise HTTPException(status_code=400, detail="Tópico ya existe")
    try:
        create_topic(topic.name, user)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al crear el tópico: {str(e)}")
    replicate_topic_to_cluster(topic.name, user)
    return {"message": f"Tópico {topic.name} creado"}

@router.delete("/{topic_name}")
def delete_topic_endpoint(topic_name: str, token: str):
    user = verify_token(token)
    topics = get_topics()
    if topic_name not in topics:
        raise HTTPException(status_code=404, detail="Tópico no encontrado")
    if topics[topic_name]["owner"] != user:
        raise HTTPException(status_code=403, detail="No autorizado para eliminar este tópico")
    try:
        delete_topic(topic_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al eliminar el tópico: {str(e)}")
    replicate_topic_deletion_to_cluster(topic_name, user)
    return {"message": f"Tópico {topic_name} eliminado"}

@router.get("/")
def list_topics_endpoint():
    topics = get_topics()
    return {"topics": list(topics.keys())}
