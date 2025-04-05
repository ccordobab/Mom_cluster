# mom_server/grpc_services/grpc_client.py
import grpc
from mom_server.grpc_services import messaging_pb2, messaging_pb2_grpc

def send_message():
    channel = grpc.insecure_channel("localhost:50051")
    stub = messaging_pb2_grpc.MessagingServiceStub(channel)

    request = messaging_pb2.MessageRequest(
        topic_name="alertas",
        sender="cliente1",
        content="¡Fuga de gas!"
    )

    response = stub.ReplicateMessage(request)
    print("Respuesta:", response.status)

if __name__ == "__main__":
    send_message()




