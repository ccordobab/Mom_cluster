# mom_server/grpc_services/grpc_server_nodo2.py
import grpc
from concurrent import futures
from mom_server.grpc_services import messaging_pb2_grpc, messaging_pb2

class MessagingService(messaging_pb2_grpc.MessagingServiceServicer):
    def ReplicateMessage(self, request, context):
        print(f"[Nodo2] Mensaje recibido:")
        print(f"Tópico: {request.topic_name}")
        print(f"Remitente: {request.sender}")
        print(f"Contenido: {request.content}")
        self.replicate_to_others(request)
        return messaging_pb2.MessageResponse(status="Mensaje replicado correctamente")

    def replicate_to_others(self, request):
        for target in ["localhost:50051", "localhost:50053"]:  # Nodo1 y Nodo3
            channel = grpc.insecure_channel(target)
            stub = messaging_pb2_grpc.MessagingServiceStub(channel)
            stub.ReplicateMessage(request)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    messaging_pb2_grpc.add_MessagingServiceServicer_to_server(MessagingService(), server)
    server.add_insecure_port('[::]:50052')  # Nodo 2 escucha en 50052
    server.start()
    print("gRPC Server Nodo2 corriendo en el puerto 50052")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()

