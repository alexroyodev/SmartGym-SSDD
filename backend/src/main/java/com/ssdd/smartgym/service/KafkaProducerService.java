package com.ssdd.smartgym.service;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void enviarAKafka(String topic, String payload) {
        kafkaTemplate.send(topic, payload);
        System.out.println("🚀 [PUENTE] Reenviado a Kafka [" + topic + "]: " + payload);
    }
}
