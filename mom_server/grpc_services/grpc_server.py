# mom_server/grpc_services/grpc_server.py
import grpc
from concurrent import futures
from mom_server.grpc_services import messaging_pb2_grpc, messaging_pb2
import sys

class MessagingService(messaging_pb2_grpc.MessagingServiceServicer):
    def __init__(self, self_port, other_nodes):
        self.self_port = self_port
        self.other_nodes = other_nodes

    def ReplicateMessage(self, request, context):
        print(f"[Nodo {self.self_port}] Recibido mensaje:")
        print(f"Tópico: {request.topic_name}")
        print(f"Remitente: {request.sender}")
        print(f"Contenido: {request.content}")
        self.replicate_to_others(request)
        return messaging_pb2.MessageResponse(status=f"Mensaje replicado desde nodo {self.self_port}")

    def replicate_to_others(self, request):
        for target in self.other_nodes:
            try:
                channel = grpc.insecure_channel(target)
                stub = messaging_pb2_grpc.MessagingServiceStub(channel)
                stub.ReplicateMessage(request)
                print(f"  → Replicado a {target}")
            except Exception as e:
                print(f"  ✖ Error replicando a {target}: {e}")

def serve(self_port, other_nodes):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    messaging_pb2_grpc.add_MessagingServiceServicer_to_server(
        MessagingService(self_port, other_nodes), server
    )
    server.add_insecure_port(f'[::]:{self_port}')
    server.start()
    print(f"gRPC Server Nodo en puerto {self_port} corriendo...")
    server.wait_for_termination()

if __name__ == "__main__":
    # Uso: python grpc_server.py <PUERTO> <OTRO1:PUERTO> <OTRO2:PUERTO>
    if len(sys.argv) != 4:
        print("Uso: python grpc_server.py <PUERTO_ACTUAL> <NODO1> <NODO2>")
        sys.exit(1)

    self_port = sys.argv[1]
    other_nodes = [sys.argv[2], sys.argv[3]]
    serve(self_port, other_nodes)
