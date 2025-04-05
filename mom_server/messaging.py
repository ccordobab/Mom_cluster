# mom_server/messaging.py
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from mom_server.auth import verify_token
import grpc
from mom_server.config import CLUSTER_NODES
from mom_server.grpc_services import messaging_pb2, messaging_pb2_grpc
import os


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
    
    # Guardar localmente
    topics[topic_name]["messages"].append({"sender": user, "content": message.content})

    # Replicar a otros nodos
    replicate_message_to_cluster(topic_name, user, message.content)

    return {"message": "Mensaje enviado y replicado"}


# Recibir mensajes de un tópico
@router.get("/messages/topic/{topic_name}")
def get_messages(topic_name: str):
    if topic_name not in topics:
        raise HTTPException(status_code=404, detail="Tópico no encontrado")
    
    return {"messages": topics[topic_name]["messages"]}


def replicate_message_to_cluster(topic_name: str, sender: str, content: str):
    self_host = os.getenv("SELF_HOST", "localhost:8000")
    grpc_port = os.getenv("GRPC_PORT", "50051")

    for node in CLUSTER_NODES:
        if node == self_host:
            continue

        grpc_address = node.replace("8000", grpc_port)
        try:
            channel = grpc.insecure_channel(grpc_address)
            stub = messaging_pb2_grpc.MessagingServiceStub(channel)

            # Primero, intentar replicar el mensaje
            request = messaging_pb2.MessageRequest(
                topic_name=topic_name,
                sender=sender,
                content=content
            )
            response = stub.ReplicateMessage(request)

            # Si el tópico no existe, crear y reintentar
            if response.status == "TOPIC_NOT_FOUND":
                # Intentar crear el tópico remotamente
                create_request = messaging_pb2.TopicRequest(
                    name=topic_name,
                    owner=sender
                )
                stub.CreateTopic(create_request)

                # Reintentar replicación
                stub.ReplicateMessage(request)

            print(f"✔️ Mensaje replicado a {grpc_address}")

        except Exception as e:
            print(f"❌ Error al replicar a {grpc_address}: {e}")
