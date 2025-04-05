# mom_server/grpc_services/grpc_server_nodo3.py
import grpc
from concurrent import futures
from mom_server.grpc_services import messaging_pb2_grpc, messaging_pb2

class MessagingService(messaging_pb2_grpc.MessagingServiceServicer):
    def ReplicateMessage(self, request, context):
        print(f"[Nodo3] Mensaje recibido para replicación:")
        print(f"Tópico: {request.topic_name}")
        print(f"Remitente: {request.sender}")
        print(f"Contenido: {request.content}")
        return messaging_pb2.MessageResponse(status="Mensaje replicado en nodo3")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    messaging_pb2_grpc.add_MessagingServiceServicer_to_server(MessagingService(), server)
    server.add_insecure_port('[::]:50053')  # Nodo 3 escucha en 50053
    server.start()
    print("gRPC Server Nodo3 corriendo en el puerto 50053")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
