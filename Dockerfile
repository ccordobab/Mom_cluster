FROM python:3.10-slim

# Variables y directorio
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
WORKDIR /app

# Copiar e instalar dependencias
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copiar el código del servidor
COPY . .

# Exponer puertos (REST:8000 + gRPC:50051)
EXPOSE 8000 50051

# Comando para levantar FastAPI + gRPC
CMD ["sh", "-c", "python3 mom_server/grpc_services/grpc_server.py & uvicorn mom_server.main:app --host 0.0.0.0 --port 8000"]
