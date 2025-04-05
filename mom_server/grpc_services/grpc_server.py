# mom_server/grpc_services/grpc_server.py

import grpc
from concurrent import futures
from mom_server.grpc_services import messaging_pb2_grpc, messaging_pb2
from mom_server.messaging import topics  # Acceso a la "base de datos" en memoria
import os

class MessagingService(messaging_pb2_grpc.MessagingServiceServicer):
    def __init__(self, self_port, other_nodes):
        self.self_port = self_port
        self.other_nodes = other_nodes

    def ReplicateMessage(self, request, context):
        print(f"[{self.self_port}] 📥 Recibido: {request.topic_name} - {request.content}")

        # ✅ Verifica si el tópico existe, si no lo crea
        if request.topic_name not in topics:
            topics[request.topic_name] = {"owner": "replicado", "messages": []}
            print(f"[{self.self_port}] 🆕 Tópico creado localmente: {request.topic_name}")

        # ✅ Guarda el mensaje replicado
        topics[request.topic_name]["messages"].append({
            "sender": request.sender,
            "content": request.content
        })
        print(f"[{self.self_port}] 💾 Mensaje guardado en tópico: {request.topic_name}")

        # 🔁 Replica a los otros nodos
        self.replicate_to_others(request)

        return messaging_pb2.MessageResponse(status=f"Recibido en {self.self_port}")

    def replicate_to_others(self, request):
        for node in self.other_nodes:
            if node == f"localhost:{self.self_port}":
                continue  # Evita replicarse a sí mismo
            try:
                channel = grpc.insecure_channel(node)
                stub = messaging_pb2_grpc.MessagingServiceStub(channel)
                stub.ReplicateMessage(request)
                print(f"[{self.self_port}] 🔁 Replicado a {node}")
            except Exception as e:
                print(f"[{self.self_port}] ❌ Error replicando a {node}: {e}")

def serve():
    self_port = os.getenv("GRPC_PORT", "50051")
    other_nodes = os.getenv("GRPC_NODES", "").split(",")

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    messaging_pb2_grpc.add_MessagingServiceServicer_to_server(
        MessagingService(self_port, other_nodes), server
    )

    server.add_insecure_port(f'[::]:{self_port}')
    server.start()
    print(f"🚀 Servidor gRPC escuchando en puerto {self_port}")
    server.wait_for_termination()


