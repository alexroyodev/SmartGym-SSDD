"""
Simulador SmartGym IoT: 3 cintas de correr (telemetría) y 1 torno de acceso.
Cada dispositivo es un cliente MQTT independiente con su propio Last Will & Testament.

Coherencia aforo-telemetría: cada atleta ENTRA por el torno, entrena en su cinta
y SALE por el torno. Una cinta solo emite pulsaciones mientras su atleta está dentro,
por lo que siempre se cumple: aforo >= atletas entrenando.
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
OTROS_SOCIOS = 5  # socios que entrenan en otras zonas (sin telemetría)

# Tiempos en segundos (ajustados para que en la demo pasen cosas)
ENTRENO = (120, 240)   # duración de una sesión en cinta
DESCANSO = (15, 40)    # tiempo fuera del gimnasio antes de volver a entrar

TOPIC_EVENTO_TORNO = f"{SEDE}/acceso_principal/{TORNO}/evento"
TOPIC_ESTADO_TORNO = f"{SEDE}/acceso_principal/{TORNO}/estado"

# Estado compartido del gimnasio: el torno es único y lo usan todos los hilos
aforo_lock = threading.Lock()
aforo = 0
torno = None  # cliente MQTT del torno (se crea en main)


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


def registrar_acceso(direccion, quien):
    """Publica un paso por el torno. El cerrojo garantiza que el orden de los
    eventos publicados coincide con el aforo real."""
    global aforo
    with aforo_lock:
        aforo += 1 if direccion == "entrada" else -1
        payload = {
            "id": str(uuid.uuid4()),
            "id_sensor": TORNO,
            "direccion": direccion,
        }
        torno.publish(TOPIC_EVENTO_TORNO, json.dumps(payload), qos=1)  # QoS 1: at-least-once
        print(f"🚪 [{TORNO}] {direccion:7} ({quien})  -> aforo: {aforo}")


def simular_cinta(maquina, usuario, parar):
    topic_estado = f"{SEDE}/sala_cardio/{maquina}/estado"
    topic = f"{SEDE}/sala_cardio/{maquina}/telemetria"
    client = crear_cliente(f"sim-{maquina}", topic_estado)

    parar.wait(random.uniform(1, 6))  # los atletas llegan de forma escalonada
    while not parar.is_set():
        # 1. El atleta entra por el torno y se sube a la cinta
        registrar_acceso("entrada", usuario)
        print(f"🏃 [{maquina}] {usuario} empieza a entrenar")

        # 2. Entrena: la cinta emite pulsaciones solo mientras él está dentro
        fin = time.time() + random.uniform(*ENTRENO)
        pulsaciones = random.uniform(95, 110)  # empieza en calentamiento
        while not parar.is_set() and time.time() < fin:
            # Paseo aleatorio con vuelta a la media (~150 ppm): el pulso sube de forma
            # progresiva y evoluciona de forma continua (rachas realistas)
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

        # 3. Termina y sale por el torno (también al parar el simulador)
        print(f"🚶 [{maquina}] {usuario} termina")
        registrar_acceso("salida", usuario)

        # 4. Pasa un rato fuera antes de volver
        parar.wait(random.uniform(*DESCANSO))

    cerrar(client, topic_estado)


def simular_otros_socios(parar):
    """Socios que entran y salen sin usar las cintas (no generan telemetría)."""
    dentro = 0
    while not parar.wait(random.uniform(8, 15)):
        if dentro == 0:
            direccion = "entrada"
        elif dentro == OTROS_SOCIOS:
            direccion = "salida"
        else:
            direccion = random.choice(["entrada", "salida"])
        dentro += 1 if direccion == "entrada" else -1
        registrar_acceso(direccion, "socio")

    # Al cerrar el gimnasio, salen todos
    for _ in range(dentro):
        registrar_acceso("salida", "socio")


if __name__ == "__main__":
    print("Arrancando simulador SmartGym IoT... (Ctrl+C para parar)")
    parar = threading.Event()

    torno = crear_cliente(f"sim-{TORNO}", TOPIC_ESTADO_TORNO)

    hilos = [threading.Thread(target=simular_cinta, args=(m, u, parar)) for m, u in CINTAS.items()]
    hilos.append(threading.Thread(target=simular_otros_socios, args=(parar,)))
    for h in hilos:
        h.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nCerrando el gimnasio: salen todos...")
        parar.set()
        for h in hilos:
            h.join()
        cerrar(torno, TOPIC_ESTADO_TORNO)  # el torno se cierra el último
        print(f"Aforo final: {aforo}")