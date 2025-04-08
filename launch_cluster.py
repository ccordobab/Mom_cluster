#!/usr/bin/env python
import os
import sys
import subprocess
import time
import signal
import argparse
import json

# Lista para mantener referencia a todos los procesos
processes = []

def signal_handler(sig, frame):
    print("\nDeteniendo todos los procesos...")
    for p in processes:
        if p.poll() is None:
            p.terminate()
    sys.exit(0)

def check_dependencies():
    required_packages = ['requests', 'fastapi', 'uvicorn', 'grpcio', 'grpcio-tools']
    for package in required_packages:
        try:
            __import__(package)
            print(f"✓ {package} ya está instalado")
        except ImportError:
            print(f"✗ {package} no está instalado. Instalando...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])
            print(f"✓ {package} instalado correctamente")

def generate_cluster_config(num_nodes=3, base_port=8000, grpc_base_port=50051):
    cluster_config = {}
    for i in range(num_nodes):
        api_port = base_port + i
        grpc_port = grpc_base_port + i
        grpc_nodes = []
        api_nodes = []
        for j in range(num_nodes):
            if j != i:
                grpc_nodes.append(f"localhost:{grpc_base_port + j}")
                api_nodes.append(f"localhost:{base_port + j}")
        cluster_config[f"node{i}"] = {
            "api_port": api_port,
            "grpc_port": grpc_port,
            "grpc_nodes": grpc_nodes,
            "api_nodes": api_nodes
        }
    with open('cluster_config.json', 'w') as f:
        json.dump(cluster_config, f)
    print("Configuración del clúster guardada en cluster_config.json")
    return cluster_config

def launch_grpc_servers(num_nodes=3, grpc_base_port=50051, api_base_port=8000):
    try:
        with open('cluster_config.json', 'r') as f:
            cluster_config = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        cluster_config = generate_cluster_config(num_nodes, api_base_port, grpc_base_port)
    
    print("\n=== Lanzando servidores GRPC ===")
    for i in range(num_nodes):
        node_config = cluster_config.get(f"node{i}", {})
        grpc_port = node_config.get('grpc_port', grpc_base_port + i)
        grpc_nodes = node_config.get('grpc_nodes', [])
        # Construir el comando: (el primer parámetro es el puerto local)
        cmd = [sys.executable, "-m", "mom_server.grpc_services.grpc_server", str(grpc_port)] + grpc_nodes
        # Configurar variables de entorno para el nodo GRPC:
        env = os.environ.copy()
        env["GRPC_PORT"] = str(grpc_port)
        env["GRPC_NODES"] = ",".join(grpc_nodes)
        env["NODE_INDEX"] = str(i)
        env["ENV_FILE"] = f".env.node{i+1}"
        env["SELF_HOST"] = f"localhost:{api_base_port + i}"
        print(f"[GRPC Node {i}] Iniciando en puerto {grpc_port} con ENV_FILE: {env['ENV_FILE']} y SELF_HOST: {env['SELF_HOST']}")
        process = subprocess.Popen(cmd, env=env)
        processes.append(process)
        time.sleep(2)
    # Delay extra para que todos los nodos GRPC se inicien
    print("Esperando 5 segundos para que los nodos GRPC se inicien completamente...")
    time.sleep(5)

def launch_api_servers(num_nodes=3, api_base_port=8000, grpc_base_port=50051):
    try:
        with open('cluster_config.json', 'r') as f:
            cluster_config = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        print("Error: No se encontró configuración del clúster")
        return

    print("\n=== Lanzando servidores API ===")
    for i in range(num_nodes):
        node_config = cluster_config.get(f"node{i}", {})
        api_port = node_config.get('api_port', api_base_port + i)
        grpc_port = node_config.get('grpc_port', grpc_base_port + i)
        api_nodes = node_config.get('api_nodes', [])
        env = os.environ.copy()
        env["PORT"] = str(api_port)
        env["SELF_HOST"] = f"localhost:{api_port}"
        env["GRPC_PORT"] = str(grpc_port)
        env["NODE_INDEX"] = str(i)
        env["CLUSTER_NODES"] = ",".join(api_nodes)
        env["ENV_FILE"] = f".env.node{i+1}"
        cmd = [sys.executable, "-m", "uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", str(api_port)]
        print(f"[API Node {i}] Iniciando en puerto {api_port} con ENV_FILE: {env['ENV_FILE']} y conectado a GRPC {grpc_port}")
        process = subprocess.Popen(cmd, env=env)
        processes.append(process)
        time.sleep(1)

def main():
    parser = argparse.ArgumentParser(description='Lanzar clúster MOM')
    parser.add_argument('--nodes', type=int, default=3, help='Número de nodos en el clúster')
    parser.add_argument('--grpc-base-port', type=int, default=50051, help='Puerto base para GRPC')
    parser.add_argument('--api-base-port', type=int, default=8000, help='Puerto base para API')
    parser.add_argument('--install-deps', action='store_true', help='Instalar dependencias necesarias')
    parser.add_argument('--config', action='store_true', help='Solo generar configuración')
    
    args = parser.parse_args()
    
    if args.install_deps:
        check_dependencies()
        return
        
    if args.config:
        generate_cluster_config(args.nodes, args.api_base_port, args.grpc_base_port)
        return
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        generate_cluster_config(args.nodes, args.api_base_port, args.grpc_base_port)
        launch_grpc_servers(args.nodes, args.grpc_base_port, args.api_base_port)
        launch_api_servers(args.nodes, args.api_base_port, args.grpc_base_port)
        print("\nClúster MOM iniciado con éxito!")
        print(f"- {args.nodes} nodos GRPC (puertos {args.grpc_base_port}-{args.grpc_base_port + args.nodes - 1})")
        print(f"- {args.nodes} nodos API (puertos {args.api_base_port}-{args.api_base_port + args.nodes - 1})")
        print("Presiona Ctrl+C para detener todos los procesos")
        for p in processes:
            p.wait()
    except Exception as e:
        print(f"Error: {str(e)}")
        for p in processes:
            if p.poll() is None:
                p.terminate()

if __name__ == "__main__":
    main()
