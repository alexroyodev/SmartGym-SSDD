package com.ssdd.smartgym.service;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
public class MqttSubscriber implements MqttCallbackExtended {

    private static final String TOPIC_TELEMETRIA = "smartgym/rivas/sala_cardio/+/telemetria";
    private static final String TOPIC_ACCESOS    = "smartgym/rivas/acceso_principal/+/evento";
    private static final String TOPIC_CONFIG     = "smartgym/rivas/config/umbral";

    @Value("${mqtt.broker.url:tcp://localhost:1883}")
    private String brokerUrl;

    // clientId FIJO: imprescindible para que el broker asocie la sesión persistente a este cliente
    @Value("${mqtt.client.id:backend-bridge}")
    private String clientId;

    private final KafkaProducerService kafkaProducer;
    private MqttAsyncClient client;

    public MqttSubscriber(KafkaProducerService kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @PostConstruct
    public void conectarYEscuchar() {
        try {
            client = new MqttAsyncClient(brokerUrl, clientId, new MemoryPersistence());
            // El callback se registra ANTES de conectar para no perder los mensajes
            // QoS 1 que el broker tenga encolados en la sesión persistente
            client.setCallback(this);

            MqttConnectOptions options = new MqttConnectOptions();
            options.setAutomaticReconnect(true);
            // Sesión persistente: el broker recuerda las suscripciones y guarda los
            // mensajes QoS 1 mientras el puente está desconectado
            options.setCleanSession(false);

            client.connect(options).waitForCompletion(10_000);
        } catch (MqttException e) {
            System.err.println("❌ Error conectando a MQTT: " + e.getMessage());
        }
    }

    /** Se ejecuta en la conexión inicial y en cada reconexión automática. */
    @Override
    public void connectComplete(boolean reconnect, String serverURI) {
        System.out.println((reconnect ? "🔄 Reconectado" : "✅ Conectado") + " a MQTT: " + serverURI);
        try {
            client.subscribe(
                new String[]{TOPIC_TELEMETRIA, TOPIC_ACCESOS, TOPIC_CONFIG},
                new int[]{0, 1, 1}   // telemetría QoS 0; accesos y configuración QoS 1
            );
        } catch (MqttException e) {
            System.err.println("❌ Error suscribiendo a MQTT: " + e.getMessage());
        }
    }

    /**
     * Si este método lanza una excepción, Paho no envía el PUBACK, se desconecta y,
     * al reconectar, el broker reentrega los mensajes QoS 1 (at-least-once real).
     */
    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        String payload = new String(message.getPayload(), StandardCharsets.UTF_8);

        if (MqttTopic.isMatched(TOPIC_TELEMETRIA, topic)) {
            kafkaProducer.enviarAKafka("kafka-telemetria", extraerClave(payload, "id_usuario"), payload);
        } else if (MqttTopic.isMatched(TOPIC_ACCESOS, topic)) {
            kafkaProducer.enviarAKafkaConfirmado("kafka-accesos", extraerClave(payload, "id_sensor"), payload);
        } else if (MqttTopic.isMatched(TOPIC_CONFIG, topic)) {
            // Clave fija: en un topic compactado Kafka conserva solo el último umbral
            kafkaProducer.enviarAKafkaConfirmado("kafka-config", "umbral", payload);
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
        System.err.println("⚠️ Conexión MQTT perdida: " + cause.getMessage() + ". Reconectando...");
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) { }

    /** Extrae la clave de partición del JSON. Si el payload es inválido devuelve null
     *  y se reenvía igualmente: el consumidor lo detectará y lo enviará a la DLQ. */
    private String extraerClave(String payload, String campo) {
        try {
            JsonObject json = JsonParser.parseString(payload).getAsJsonObject();
            JsonElement valor = json.get(campo);
            return (valor == null || valor.isJsonNull()) ? null : valor.getAsString();
        } catch (Exception e) {
            return null;
        }
    }

    @PreDestroy
    public void desconectar() {
        try {
            if (client != null && client.isConnected()) client.disconnect();
        } catch (MqttException ignored) { }
    }
}