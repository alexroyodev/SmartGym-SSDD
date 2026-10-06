package com.ssdd.smartgym.service;

import jakarta.annotation.PostConstruct;
import org.eclipse.paho.client.mqttv3.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class MqttSubscriber {

    @Value("${mqtt.broker.url:tcp://localhost:1883}")
    private String brokerUrl;

    @Value("${mqtt.client.id:backend-bridge}")
    private String clientId;

    private final KafkaProducerService kafkaProducer;

    public MqttSubscriber(KafkaProducerService kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @PostConstruct
    public void conectarYEscuchar() {
        try {
            // Nos conectamos a Mosquitto
            MqttClient client = new MqttClient(brokerUrl, clientId);
            MqttConnectOptions options = new MqttConnectOptions();
            options.setAutomaticReconnect(true);
            options.setCleanSession(true);
            
            client.connect(options);
            System.out.println("✅ Conectado con éxito a MQTT: " + brokerUrl);

            // Nos suscribimos a la Telemetría (QoS 0 - At-most-once)
            client.subscribe("smartgym/rivas/sala_cardio/+/telemetria", 0, (topic, message) -> {
                String payload = new String(message.getPayload());
                kafkaProducer.enviarAKafka("kafka-telemetria", payload);
            });

            // Nos suscribimos a los Tornos (QoS 1 - At-least-once)
            client.subscribe("smartgym/rivas/acceso_principal/+/evento", 1, (topic, message) -> {
                String payload = new String(message.getPayload());
                kafkaProducer.enviarAKafka("kafka-accesos", payload);
            });

            // Nos suscribimos a la Configuración del Slider (QoS 1 - Crítico)
            client.subscribe("smartgym/rivas/config/umbral", 1, (topic, message) -> {
                String payload = new String(message.getPayload());
                kafkaProducer.enviarAKafka("kafka-config", payload);
            });

        } catch (MqttException e) {
            System.err.println("❌ Error conectando a MQTT: " + e.getMessage());
        }
    }
}