package com.ssdd.smartgym.service;

import jakarta.annotation.PostConstruct;
import org.eclipse.paho.client.mqttv3.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class MqttSubscriber {

    @Value("${mqtt.broker.url}")
    private String brokerUrl;

    @Value("${mqtt.client.id}")
    private String clientId;

    @Value("${mqtt.topic.telemetria}")
    private String topicTelemetria;

    @Value("${mqtt.topic.accesos}")
    private String topicAccesos;

    private final KafkaProducerService kafkaProducer;

    public MqttSubscriber(KafkaProducerService kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @PostConstruct
    public void conectarYEscuchar() {
        try {
            // 1. Nos conectamos a Mosquitto
            MqttClient client = new MqttClient(brokerUrl, clientId);
            MqttConnectOptions options = new MqttConnectOptions();
            options.setAutomaticReconnect(true);
            options.setCleanSession(true);
            
            client.connect(options);
            System.out.println("✅ Conectado con éxito a MQTT: " + brokerUrl);

            // 2. Nos suscribimos al topic de Telemetría (las máquinas)
            client.subscribe(topicTelemetria, (topic, message) -> {
                String payload = new String(message.getPayload());
                // Lo mandamos al topic de Kafka equivalente
                kafkaProducer.enviarAKafka("kafka-telemetria", payload);
            });

            // 3. Nos suscribimos al topic de Accesos (los tornos)
            client.subscribe(topicAccesos, (topic, message) -> {
                String payload = new String(message.getPayload());
                // Lo mandamos al topic de Kafka equivalente
                kafkaProducer.enviarAKafka("kafka-accesos", payload);
            });

        } catch (MqttException e) {
            System.err.println("❌ Error conectando a MQTT: " + e.getMessage());
        }
    }
}
