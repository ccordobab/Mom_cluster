# mom_server/main.py
from fastapi import FastAPI
from mom_server.auth import router as auth_router
from mom_server.messaging import router as messaging_router
from mom_server.cluster import router as cluster_router

app = FastAPI(title="MOM Cluster")

# Registrar las rutas de autenticación, mensajería y clúster
app.include_router(auth_router, prefix="/auth", tags=["Authentication"])
app.include_router(messaging_router, prefix="/messages", tags=["Messaging"])
app.include_router(cluster_router, prefix="/cluster", tags=["Cluster"])

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
