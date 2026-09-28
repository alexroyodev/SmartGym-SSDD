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
            // 1. Nos conectamos a Mosquitto
            MqttClient client = new MqttClient(brokerUrl, clientId);
            MqttConnectOptions options = new MqttConnectOptions();
            options.setAutomaticReconnect(true);
            options.setCleanSession(true);
            
            client.connect(options);
            System.out.println("✅ Conectado con éxito a MQTT: " + brokerUrl);

            // 2. Nos suscribimos a TODAS las cintas usando el comodín (+)
            client.subscribe("smartgym/rivas/sala_cardio/+/telemetria", (topic, message) -> {
                String payload = new String(message.getPayload());
                // Lo mandamos al topic de Kafka equivalente
                kafkaProducer.enviarAKafka("kafka-telemetria", payload);
            });

            // 3. Nos suscribimos a TODOS los tornos usando el comodín (+)
            client.subscribe("smartgym/rivas/acceso_principal/+/evento", (topic, message) -> {
                String payload = new String(message.getPayload());
                // Lo mandamos al topic de Kafka equivalente
                kafkaProducer.enviarAKafka("kafka-accesos", payload);
            });

        } catch (MqttException e) {
            System.err.println("❌ Error conectando a MQTT: " + e.getMessage());
        }
    }
}