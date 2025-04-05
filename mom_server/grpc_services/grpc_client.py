# mom_server/grpc_services/grpc_client.py

import grpc
from mom_server.grpc_services import messaging_pb2, messaging_pb2_grpc

def run():
    # Nos conectamos solo al nodo2 (puerto 50052)
    channel = grpc.insecure_channel('localhost:50052')
    stub = messaging_pb2_grpc.MessagingServiceStub(channel)

    request = messaging_pb2.MessageRequest(
        topic_name='alertas',
        sender='cliente1',
        content='⚠️ Hay una fuga de agua en el edificio.'
    )

    response = stub.ReplicateMessage(request)
    print("Respuesta del servidor:", response.status)

if __name__ == '__main__':
    run()



