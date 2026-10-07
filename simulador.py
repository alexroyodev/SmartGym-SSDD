"""
Simulador SmartGym IoT: 3 cintas de correr (telemetría) y 1 torno de acceso.
Cada dispositivo es un cliente MQTT independiente con su propio Last Will & Testament.
"""
import json
import random
import threading
import time
import uuid

import paho.mqtt.client as mqtt

BROKER = "localhost"
PORT = 1883
KEEPALIVE = 10  # segundos; el broker da por caído un cliente tras 1,5 × keepalive sin tráfico

SEDE = "smartgym/rivas"
CINTAS = {"cinta_01": "u101", "cinta_02": "u102", "cinta_03": "u103"}  # máquina -> atleta
TORNO = "torno_01"


def crear_cliente(client_id, topic_estado):
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)

    # LWT: si el dispositivo se desconecta de forma abrupta, el BROKER publica "offline"
    client.will_set(topic_estado, "offline", qos=1, retain=True)

    def on_connect(c, userdata, flags, reason_code, properties):
        # Estado retenido: un suscriptor que llegue más tarde conoce el estado actual
        c.publish(topic_estado, "online", qos=1, retain=True)
        print(f"✅ [{client_id}] conectado ({reason_code})")

    client.on_connect = on_connect
    client.connect(BROKER, PORT, KEEPALIVE)
    client.loop_start()  # hilo de red: procesa PUBACK, keepalive y reconexiones
    return client


def cerrar(client, topic_estado):
    # Apagado ordenado: el propio dispositivo avisa (el LWT solo salta en caídas abruptas)
    client.publish(topic_estado, "offline", qos=1, retain=True).wait_for_publish(timeout=5)
    client.disconnect()
    client.loop_stop()


def simular_cinta(maquina, usuario, parar):
    topic_estado = f"{SEDE}/sala_cardio/{maquina}/estado"
    topic = f"{SEDE}/sala_cardio/{maquina}/telemetria"
    client = crear_cliente(f"sim-{maquina}", topic_estado)

    pulsaciones = random.uniform(120, 140)
    while not parar.is_set():
        # Paseo aleatorio con vuelta a la media (~150 ppm): el pulso evoluciona de forma
        # continua, por lo que las rachas por encima del umbral son realistas
        pulsaciones += random.uniform(-8, 8) + (150 - pulsaciones) * 0.1
        pulsaciones = max(90, min(200, pulsaciones))

        payload = {
            "id": str(uuid.uuid4()),  # id único de evento -> idempotencia aguas abajo
            "id_usuario": usuario,
            "pulsaciones": round(pulsaciones),
        }
        client.publish(topic, json.dumps(payload), qos=0)  # QoS 0: at-most-once
        print(f"📡 [{maquina}] {payload}")
        parar.wait(2)

    cerrar(client, topic_estado)


def simular_torno(parar):
    topic_estado = f"{SEDE}/acceso_principal/{TORNO}/estado"
    topic = f"{SEDE}/acceso_principal/{TORNO}/evento"
    client = crear_cliente(f"sim-{TORNO}", topic_estado)

    dentro = 0
    while not parar.is_set():
        # Solo puede salir alguien si hay gente dentro
        direccion = "entrada" if dentro == 0 else random.choice(["entrada", "salida"])
        dentro += 1 if direccion == "entrada" else -1

        payload = {
            "id": str(uuid.uuid4()),
            "id_sensor": TORNO,
            "direccion": direccion,
        }
        client.publish(topic, json.dumps(payload), qos=1)  # QoS 1: at-least-once
        print(f"🚪 [{TORNO}] {payload}  (dentro: {dentro})")
        parar.wait(10)

    cerrar(client, topic_estado)


if __name__ == "__main__":
    print("Arrancando simulador SmartGym IoT... (Ctrl+C para parar)")
    parar = threading.Event()

    hilos = [threading.Thread(target=simular_cinta, args=(m, u, parar)) for m, u in CINTAS.items()]
    hilos.append(threading.Thread(target=simular_torno, args=(parar,)))
    for h in hilos:
        h.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nParando dispositivos...")
        parar.set()
        for h in hilos:
            h.join()