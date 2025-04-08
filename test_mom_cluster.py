#!/usr/bin/env python
# test_mom_cluster.py

import requests
import json
import time
import sys
import random
import os

# URLs de los nodos para pruebas
BASE_URLS = [
    "http://localhost:8000",
    "http://localhost:8001",
    "http://localhost:8002"
]

def print_header(message):
    print("\n" + "=" * 60)
    print(f" {message} ".center(60, "="))
    print("=" * 60)

def print_step(message):
    print(f"\n➡️ {message}")

def register_user(base_url, username, password):
    print_step(f"Registrando usuario '{username}' en {base_url}")
    url = f"{base_url}/auth/register"
    data = {"username": username, "password": password}
    response = requests.post(url, json=data)
    print(f"Estado: {response.status_code}")
    print(f"Respuesta: {response.json()}")
    return response.status_code == 200

def login_user(base_url, username, password):
    print_step(f"Iniciando sesión con usuario '{username}' en {base_url}")
    url = f"{base_url}/auth/login"
    data = {"username": username, "password": password}
    response = requests.post(url, json=data)
    print(f"Estado: {response.status_code}")
    
    if response.status_code == 200:
        token = response.json().get("token")
        print(f"Token obtenido: {token[:15]}...")
        return token
    else:
        print(f"Error: {response.json()}")
        return None

def create_topic(base_url, token, topic_name, owner):
    print_step(f"Creando tópico '{topic_name}' en {base_url}")
    url = f"{base_url}/messages/topics"
    data = {"name": topic_name, "owner": owner}
    response = requests.post(url, json=data, params={"token": token})
    print(f"Estado: {response.status_code}")
    print(f"Respuesta: {response.json()}")
    return response.status_code == 200

def list_topics(base_url):
    print_step(f"Listando tópicos en {base_url}")
    url = f"{base_url}/messages/topics"
    response = requests.get(url)
    print(f"Estado: {response.status_code}")
    topics = response.json().get("topics", [])
    print(f"Tópicos: {topics}")
    return topics

def send_message(base_url, token, topic_name, sender, content):
    print_step(f"Enviando mensaje al tópico '{topic_name}' en {base_url}")
    url = f"{base_url}/messages/messages/topic/{topic_name}"
    data = {"sender": sender, "content": content}
    response = requests.post(url, json=data, params={"token": token})
    print(f"Estado: {response.status_code}")
    print(f"Respuesta: {response.json()}")
    return response.status_code == 200

def get_messages(base_url, topic_name):
    print_step(f"Obteniendo mensajes del tópico '{topic_name}' en {base_url}")
    url = f"{base_url}/messages/messages/topic/{topic_name}"
    response = requests.get(url)
    print(f"Estado: {response.status_code}")
    messages = response.json().get("messages", [])
    print(f"Mensajes: {messages}")
    return messages

def create_queue(base_url, token, queue_name, owner):
    print_step(f"Creando cola '{queue_name}' en {base_url}")
    url = f"{base_url}/messages/queues"
    data = {"name": queue_name, "owner": owner}
    response = requests.post(url, json=data, params={"token": token})
    print(f"Estado: {response.status_code}")
    print(f"Respuesta: {response.json()}")
    return response.status_code == 200

def list_queues(base_url):
    print_step(f"Listando colas en {base_url}")
    url = f"{base_url}/messages/queues"
    response = requests.get(url)
    print(f"Estado: {response.status_code}")
    queues = response.json().get("queues", [])
    print(f"Colas: {queues}")
    return queues

def send_queue_message(base_url, token, queue_name, sender, content):
    print_step(f"Enviando mensaje a la cola '{queue_name}' en {base_url}")
    url = f"{base_url}/messages/messages/queue/{queue_name}"
    data = {"sender": sender, "content": content}
    response = requests.post(url, json=data, params={"token": token})
    print(f"Estado: {response.status_code}")
    print(f"Respuesta: {response.json()}")
    return response.status_code == 200

def get_queue_message(base_url, token, queue_name):
    print_step(f"Consumiendo mensaje de la cola '{queue_name}' en {base_url}")
    url = f"{base_url}/messages/messages/queue/{queue_name}"
    response = requests.get(url, params={"token": token})
    print(f"Estado: {response.status_code}")
    print(f"Respuesta: {response.json()}")
    return response.json().get("message")

def check_replication(expected_item, items, item_type="tópico"):
    if expected_item in items:
        print(f"✅ {item_type.capitalize()} '{expected_item}' replicado correctamente")
        return True
    else:
        print(f"❌ {item_type.capitalize()} '{expected_item}' NO replicado")
        return False

def run_tests():
    print_header("INICIANDO PRUEBAS DEL CLÚSTER MOM")
    
    # Generar nombres únicos para evitar conflictos
    test_id = random.randint(1000, 9999)
    username1 = f"usuario{test_id}_1"
    username2 = f"usuario{test_id}_2"
    topic_name = f"topic{test_id}"
    queue_name = f"queue{test_id}"
    
    # 1. Registro de usuarios
    print_header("PRUEBA DE REGISTRO Y AUTENTICACIÓN")
    register_user(BASE_URLS[0], username1, "pass123")
    register_user(BASE_URLS[1], username2, "pass123")
    
    # 2. Login de usuarios
    token1 = login_user(BASE_URLS[0], username1, "pass123")
    token2 = login_user(BASE_URLS[1], username2, "pass123")
    
    if not token1 or not token2:
        print("❌ Error al obtener tokens de autenticación. Pruebas detenidas.")
        return
    
    # 3. Creación y replicación de tópicos
    print_header("PRUEBA DE CREACIÓN Y REPLICACIÓN DE TÓPICOS")
    create_topic(BASE_URLS[0], token1, topic_name, username1)
    
    # Esperar por replicación
    print("Esperando a que se replique el tópico...")
    time.sleep(2)
    
    # Verificar replicación en todos los nodos
    all_replicated = True
    for url in BASE_URLS:
        topics = list_topics(url)
        if not check_replication(topic_name, topics, "tópico"):
            all_replicated = False
    
    if all_replicated:
        print("✅ Replicación de tópicos funciona correctamente")
    else:
        print("❌ Problema en la replicación de tópicos")
    
    # 4. Envío de mensajes a tópicos y verificación de replicación
    print_header("PRUEBA DE MENSAJES EN TÓPICOS")
    message_content = f"Mensaje de prueba {test_id}"
    send_message(BASE_URLS[0], token1, topic_name, username1, message_content)
    
    # Esperar por replicación
    print("Esperando a que se replique el mensaje...")
    time.sleep(2)
    
    # Verificar replicación de mensajes
    message_replicated = True
    for url in BASE_URLS:
        messages = get_messages(url, topic_name)
        message_found = False
        for msg in messages:
            if msg.get("content") == message_content:
                message_found = True
                break
        
        if message_found:
            print(f"✅ Mensaje replicado correctamente en {url}")
        else:
            print(f"❌ Mensaje NO replicado en {url}")
            message_replicated = False
    
    if message_replicated:
        print("✅ Replicación de mensajes funciona correctamente")
    else:
        print("❌ Problema en la replicación de mensajes")
    
    # 5. Prueba de colas
    if "get_queue_message" in globals():  # Solo ejecutar si la función existe
        print_header("PRUEBA DE COLAS")
        create_queue(BASE_URLS[0], token1, queue_name, username1)
        
        # Esperar por replicación
        print("Esperando a que se replique la cola...")
        time.sleep(2)
        
        # Verificar replicación en todos los nodos
        queue_replicated = True
        for url in BASE_URLS:
            queues = list_queues(url)
            if not check_replication(queue_name, queues, "cola"):
                queue_replicated = False
        
        if queue_replicated:
            print("✅ Replicación de colas funciona correctamente")
        else:
            print("❌ Problema en la replicación de colas")
        
        # Enviar mensajes a cola
        queue_message = f"Mensaje de cola {test_id}"
        send_queue_message(BASE_URLS[0], token1, queue_name, username1, queue_message)
        
        # Verificar que solo un consumidor recibe el mensaje
        msg1 = get_queue_message(BASE_URLS[0], token1, queue_name)
        if msg1 and msg1.get("content") == queue_message:
            print("✅ Mensaje consumido correctamente de la cola")
        else:
            print("❌ Problema al consumir mensaje de la cola")
        
        # No debería haber más mensajes
        msg2 = get_queue_message(BASE_URLS[0], token1, queue_name)
        if not msg2 or not msg2.get("content"):
            print("✅ Cola vacía después de consumir (correcto)")
        else:
            print("❌ La cola debería estar vacía pero no lo está")
    
    print_header("PRUEBAS COMPLETADAS")
    print("Resultados:")
    print("1. Registro y autenticación: ✅ Funcionando")
    print(f"2. Replicación de tópicos: {'✅ Funcionando' if all_replicated else '❌ Problema'}")
    print(f"3. Replicación de mensajes: {'✅ Funcionando' if message_replicated else '❌ Problema'}")
    if "get_queue_message" in globals():
        print(f"4. Replicación de colas: {'✅ Funcionando' if queue_replicated else '❌ Problema'}")
    print("\n¡Pruebas completadas!")

if __name__ == "__main__":
    run_tests()
