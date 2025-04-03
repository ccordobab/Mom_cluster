# mom_server/messaging.py
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from mom_server.auth import verify_token

router = APIRouter()

# Simulación de base de datos en memoria
topics = {}
queues = {}

# Modelos para mensajes
class TopicQueue(BaseModel):
    name: str
    owner: str

class Message(BaseModel):
    sender: str
    content: str

# Crear un tópico
@router.post("/topics")
def create_topic(topic: TopicQueue, token: str):
    user = verify_token(token)
    if topic.name in topics:
        raise HTTPException(status_code=400, detail="Tópico ya existe")
    
    topics[topic.name] = {"owner": user, "messages": []}
    return {"message": f"Tópico {topic.name} creado"}

# Listar tópicos
@router.get("/topics")
def list_topics():
    return {"topics": list(topics.keys())}

# Enviar mensaje a un tópico
@router.post("/messages/topic/{topic_name}")
def send_message(topic_name: str, message: Message, token: str):
    user = verify_token(token)
    if topic_name not in topics:
        raise HTTPException(status_code=404, detail="Tópico no encontrado")
    
    topics[topic_name]["messages"].append({"sender": user, "content": message.content})
    return {"message": "Mensaje enviado"}

# Recibir mensajes de un tópico
@router.get("/messages/topic/{topic_name}")
def get_messages(topic_name: str):
    if topic_name not in topics:
        raise HTTPException(status_code=404, detail="Tópico no encontrado")
    
    return {"messages": topics[topic_name]["messages"]}
