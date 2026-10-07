package com.ssdd.smartgym.service;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
public class KafkaProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Envío sin esperar confirmación (telemetría, QoS 0).
     * La clave garantiza que todos los mensajes de una misma entidad van a la misma partición.
     */
    public void enviarAKafka(String topic, String key, String payload) {
        kafkaTemplate.send(topic, key, payload).whenComplete((resultado, ex) -> {
            if (ex != null) {
                System.err.println("[PUENTE] Fallo enviando a Kafka [" + topic + "]: " + ex.getMessage());
            }
        });
        System.out.println("[PUENTE] Reenviado a Kafka [" + topic + "] key=" + key + ": " + payload);
    }

    /**
     * Envío confirmado (accesos y configuración, QoS 1).
     * Bloquea hasta que Kafka confirma la escritura (acks=all). Si falla, lanza excepción:
     * así el puente NO confirma el mensaje MQTT y el broker lo reentrega.
     */
    public void enviarAKafkaConfirmado(String topic, String key, String payload) throws Exception {
        kafkaTemplate.send(topic, key, payload).get(10, TimeUnit.SECONDS);
        System.out.println("[PUENTE] Confirmado por Kafka [" + topic + "] key=" + key + ": " + payload);
    }
}