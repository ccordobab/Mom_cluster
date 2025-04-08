# api/main.py

from fastapi import FastAPI
from api.routers import topics, queues, messages, auth

app = FastAPI(title="MOM Cluster API")

# Incluir los routers con sus prefijos actualizados para coincidir con las pruebas
app.include_router(auth.router)
app.include_router(topics.router, prefix="/messages/topics", tags=["Topics"])
app.include_router(queues.router, prefix="/messages/queues", tags=["Queues"])
app.include_router(messages.router, prefix="/messages/messages", tags=["Messages"])

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)