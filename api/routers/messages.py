# api/routers/messages.py

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from api.routers.auth import verify_token
from mom_server.services.state import (
    get_topics,
    add_topic_message,
    get_queues,
    add_queue_message,
    consume_queue_message
)
from mom_server.services.messaging import replicate_message_to_cluster

router = APIRouter()

class Message(BaseModel):
    sender: str
    content: str

@router.post("/topic/{topic_name}")
def send_message_endpoint(topic_name: str, message: Message, token: str):
    user = verify_token(token)
    topics = get_topics()
    if topic_name not in topics:
        raise HTTPException(status_code=404, detail="Tópico no encontrado")
    try:
        add_topic_message(topic_name, user, message.content)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al agregar mensaje al tópico: {str(e)}")
    replicate_message_to_cluster(topic_name, user, message.content)
    return {"message": "Mensaje enviado y replicado"}

@router.post("/queue/{queue_name}")
def send_queue_message_endpoint(queue_name: str, message: Message, token: str):
    user = verify_token(token)
    queues = get_queues()
    if queue_name not in queues:
        raise HTTPException(status_code=404, detail="Cola no encontrada")
    try:
        add_queue_message(queue_name, user, message.content)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al agregar mensaje a la cola: {str(e)}")
    return {"message": "Mensaje enviado a la cola"}

@router.get("/topic/{topic_name}")
def get_messages_endpoint(topic_name: str):
    topics = get_topics()
    if topic_name not in topics:
        raise HTTPException(status_code=404, detail="Tópico no encontrado")
    return {"messages": topics[topic_name]["messages"]}

@router.get("/queue/{queue_name}")
def get_queue_message_endpoint(queue_name: str, token: str):
    user = verify_token(token)
    queues = get_queues()
    if queue_name not in queues:
        raise HTTPException(status_code=404, detail="Cola no encontrada")
    
    msg = consume_queue_message(queue_name)
    if not msg:
        return {"message": None}
    
    return {"message": msg}
