package com.ssdd.smartgym.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Component
public class MqttAlertPublisher {

    private static final String TOPIC_ALERTAS = "smartgym/rivas/alertas/medicas";

    @Value("${mqtt.broker.url:tcp://localhost:1883}")
    private String brokerUrl;

    private MqttAsyncClient client;

    @PostConstruct
    public void conectar() {
        try {
            // clientId único por instancia: varias réplicas del backend pueden publicar alertas a la vez
            client = new MqttAsyncClient(brokerUrl, "backend-alertas-" + UUID.randomUUID(), new MemoryPersistence());
            MqttConnectOptions options = new MqttConnectOptions();
            options.setAutomaticReconnect(true);
            options.setCleanSession(true);
            client.connect(options).waitForCompletion(10_000);
            System.out.println("✅ Publicador de alertas conectado a MQTT");
        } catch (MqttException e) {
            System.err.println("❌ Error conectando el publicador de alertas: " + e.getMessage());
        }
    }

    public void publicarAlerta(String mensaje) {
        try {
            if (client == null || !client.isConnected()) {
                System.err.println("❌ Alerta no enviada (MQTT desconectado): " + mensaje);
                return;
            }
            MqttMessage msg = new MqttMessage(mensaje.getBytes(StandardCharsets.UTF_8));
            msg.setQos(1);         // alerta médica: at-least-once
            msg.setRetained(false); // una alerta antigua no debe mostrarse al reconectar
            client.publish(TOPIC_ALERTAS, msg);
        } catch (MqttException e) {
            System.err.println("❌ Error publicando alerta: " + e.getMessage());
        }
    }

    @PreDestroy
    public void desconectar() {
        try {
            if (client != null && client.isConnected()) client.disconnect();
        } catch (MqttException ignored) { }
    }
}