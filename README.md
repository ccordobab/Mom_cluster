# MOM Cluster 🚀
**Middleware Orientado a Mensajes Distribuido**

> Universidad EAFIT •  *Tópicos Especiales en Tele﻿mática* • 2025

---

## ✨ Resumen
MOM Cluster es un middleware distribuido y tolerante a fallos que ofrece comunicación asíncrona entre aplicaciones mediante *tópicos* y *colas*. Utiliza transportes híbridos:

- **REST API** — cliente ↔ MOM  
- **gRPC** — MOM ↔ MOM (replicación y coordinación interna)

Soporta particionamiento, replicación, autenticación vía JWT y persistencia en SQLite. Deployable en AWS Academy.

---

## 📑 Tabla de Contenidos
0. [Wiki](#-wiki)  
1. [Características](#-características)  
2. [Arquitectura](#-arquitectura)  
3. [Requisitos](#-requisitos)  
4. [Instalación](#-instalación)  
5. [Configuración](#-configuración)  
6. [Uso](#-uso)  
7. [Referencia de API REST](#-referencia-de-api-rest)  
8. [Estructura del proyecto](#-estructura-del-proyecto)  
9. [Hoja de ruta y limitaciones](#-hoja-de-ruta-y-limitaciones) 

---

## 📖 Documentación Detallada

Toda la información del análisis, diseño e implementación está ampliada en la **Wiki del proyecto**.  
➡️  [Ir a la Wiki](https://github.com/ccordobab/Mom_cluster/wiki)

---

## 🚀 Características
| Categoría                | Detalle                                                                 |
|--------------------------|-------------------------------------------------------------------------|
| **Arquitectura en clúster** | Escalado horizontal con N ≥ 3 nodos para alta disponibilidad            |
| **Transporte híbrido**      | REST (HTTP) externo, gRPC interno                                      |
| **Particionamiento**        | Algoritmo de hash consistente para balancear tópicos y colas           |
| **Replicación**             | Primario → secundarios para durabilidad                                 |
| **Persistencia**            | SQLite                             |
| **Autenticación**           | JWT, contraseñas hashed con PBKDF2                                     |
| **Operaciones**             | Crear/Listar/Eliminar tópicos y colas; Publicar/Suscribir; Enviar/Recibir FIFO |
| **CLI**                     | `launch_cluster.py` inicia clúster local o en AWS                      |
| **Tests**                   | Pytest: cluster, persistencia, particionamiento                        |

---

## 🏗️ Arquitectura
```text
                       ┌───────────────┐
                       │    Clientes    │  REST (JWT)
                       └───────┬────────┘
                               │
                               ▼
┌────────────────────────────────────────────────────────┐
│                     MOM Cluster                        │ 
│  ┌──────────┐  gRPC  ┌──────────┐  gRPC  ┌──────────┐  │
│  │  Nodo 1  │◄──────►│  Nodo 2  │◄──────►│  Nodo 3  │  │
│  │API+gRPC+│         │API+gRPC+ │        │API+gRPC+ │  │
│  │SQLite   │         │SQLite    │        │SQLite    │  │
│  └──────────┘        └──────────┘        └──────────┘  │
└────────────────────────────────────────────────────────┘
```

**Flujo de datos:**  
1. Cliente llama a cualquier nodo via REST.  
2. Nodo verifica ownership (hash consistente).  
3. Si no es responsable → reenvía al nodo adecuado.  
4. Escrituras se replican por gRPC a secundarios.  
5. Lecturas atendidas por réplicas responsables.

---

## 📋 Requisitos
- Python 3.8+  
- FastAPI & Uvicorn  
- gRPC & protobuf  
- SQLite (por defecto)  
- PyJWT  
- Requests (para pruebas)

---

## ⚡ Instalación
```bash
git clone https://github.com/ccordobab/Mom_clusterr.git
cd mom-cluster
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

---

## ⚙️ Configuración
```bash
cp .env.example .env
# Ajusta .env según necesites
python launch_cluster.py --help
```

---

## ▶️ Uso
### Iniciar clúster local (3 nodos)
```bash
python launch_cluster.py --nodes 3
```

### Ejecutar tests
```bash
python test_mom_cluster.py
python test_mom_persistence.py
python test_partitioning.py
```

---

## 📚 Referencia de API REST
### **Auth**
<details>
<summary>Ver endpoints</summary>

| Método  | Ruta               | Body / Headers            | Descripción       |
|---------|--------------------|---------------------------|-------------------|
| POST    | /auth/register     | { "username", "password"} | Registrar usuario |
| POST    | /auth/login        | ↑                         | Obtener JWT       |
| GET     | /auth/users        | Authorization: Bearer …   | Listar usuarios   |
</details>

### **Tópicos**
<details>
<summary>Ver endpoints</summary>

- GET    /messages/topics  
- POST   /messages/topics       { "name": "" }  
- DELETE /messages/topics/{name}  
- POST   /messages/messages/topic/{name}  { "data": "..." }  
- GET    /messages/messages/topic/{name}  (SSE)  
</details>

### **Colas**
<details>
<summary>Ver endpoints</summary>

- GET    /messages/queues  
- POST   /messages/queues      { "name": "" }  
- DELETE /messages/queues/{name}  
- POST   /messages/messages/queue/{name}  { "data": "..." }  
- GET    /messages/messages/queue/{name}  (FIFO)  
</details>


---

## 🗂️ Estructura del proyecto
```text
mom-cluster/
├── api/
│   ├── routers/
│   └── main.py
├── mom_server/
│   ├── cluster.py
│   ├── messaging.py
│   ├── partitioning.py
│   ├── state.py
│   └── grpc_services/
├── launch_cluster.py
├── tests/
├── example_app/
└── requirements.txt
```

---


**Equipo**  
- Camilo Cordoba / ccordobab@eafit.edu.co 

- Juan Esteban Garcia / jegarciag1@eafit.edu.co 

- Nicolas Peña / npenaj@eafit.edu.co 

- Verónica Zapata / vzapatav@eafit.edu.co 

---
