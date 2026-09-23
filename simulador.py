import paho.mqtt.client as mqtt
import json
import time
import random
import threading

# Configuración del Broker MQTT (Mosquitto en Docker)
BROKER = "localhost"
PORT = 1883
TOPIC_TELEMETRIA = "smartgym/telemetria"
TOPIC_ACCESOS = "smartgym/accesos"

client = mqtt.Client()
client.connect(BROKER, PORT, 60)

def simular_telemetria():
    usuarios = ["u101", "u102", "u103"]
    while True:
        for user in usuarios:
            payload = {
                "id_usuario": user,
                "pulsaciones": random.randint(120, 185) # Simula latidos
            }
            client.publish(TOPIC_TELEMETRIA, json.dumps(payload), qos=0)
            print(f"📡 [MQTT] Telemetría enviada: {payload}")
        time.sleep(2) # Envía datos cada 2 segundos

def simular_accesos():
    while True:
        payload = {
            "id_sensor": "torno_principal",
            "direccion": random.choice(["entrada", "salida"])
        }
        client.publish(TOPIC_ACCESOS, json.dumps(payload), qos=1)
        print(f"🚪 [MQTT] Acceso enviado: {payload}")
        time.sleep(10) # Envía un acceso aleatorio cada 10 segundos

# Iniciamos los dos hilos en paralelo
print("Arrancando simulador SmartGym IoT...")
hilo_telemetria = threading.Thread(target=simular_telemetria)
hilo_accesos = threading.Thread(target=simular_accesos)

hilo_telemetria.start()
hilo_accesos.start()