🏋️‍♂️ SmartGym IoT - Telemetría y Aforo en Tiempo Real
Este proyecto simula el sistema IoT de un gimnasio inteligente. Captura datos biométricos de los usuarios (pulsaciones en las máquinas) y eventos de acceso (tornos), procesándolos en tiempo real mediante una arquitectura de streaming y almacenamiento distribuido.

🛠️ Requisitos previos
Para levantar este proyecto en tu máquina, necesitas tener instalado:

Docker y Docker Compose (Para la infraestructura).

Java 17 o superior (Para el backend de Spring Boot).

Python 3 (Para ejecutar el simulador de sensores).

🚀 Cómo arrancar el proyecto paso a paso
Paso 1: Levantar la infraestructura (Docker)
Abre una terminal en la raíz del proyecto y ejecuta:

Bash
docker-compose up -d
Esto descargará y arrancará Mosquitto (MQTT), Apache Kafka, MongoDB y Node-RED.

Paso 2: Inicializar la base de datos distribuida
Como usamos MongoDB en modo "Replica Set" (distribuido), hay que inicializarlo la primera vez. Ejecuta este comando en la terminal:

Bash
docker exec -it smartgym_mongo mongosh --eval "rs.initiate()"
Si devuelve un ok: 1, la base de datos está lista.

Paso 3: Arrancar el Backend (Puente y Persistencia)
El backend en Java captura los datos de MQTT, los envía a Kafka y los guarda en MongoDB.
Abre una terminal en la carpeta de tu backend y ejecuta:

Bash
# En Windows:
.\mvnw spring-boot:run

# En Mac/Linux:
./mvnw spring-boot:run
Paso 4: Encender los "Sensores" (Simulador Python)
Vamos a generar los datos falsos. Abre una terminal nueva en la raíz del proyecto, instala la librería necesaria y ejecuta el script:

Bash
pip install paho-mqtt
python simulador.py
Verás en la consola de Java cómo los datos empiezan a llegar y a guardarse en la base de datos a toda velocidad.

Paso 5: Ver el Panel Visual en Tiempo Real (Node-RED)
Abre tu navegador web y entra en:
👉 http://localhost:1880/ui

Allí podrás ver las gráficas y el panel de control actualizándose en vivo con los datos de las pulsaciones y los accesos del gimnasio.

Para detener todo:
Pulsa Ctrl + C en la terminal del simulador y del backend, y ejecuta docker-compose down para apagar los contenedores.