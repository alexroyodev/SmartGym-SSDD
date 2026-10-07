package com.ssdd.smartgym.config;

import com.ssdd.smartgym.service.EventoInvalidoException;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaErrorConfig {

    /**
     * Distingue dos tipos de error en los consumidores:
     *  - Permanentes (evento mal formado, bug): se envían directamente a <topic>-dlq.
     *  - Transitorios (MongoDB caído): se reintenta cada 5 s sin límite. Mientras tanto
     *    el offset NO se confirma y Kafka retiene los eventos hasta que la BD vuelve.
     * Spring Boot aplica este bean automáticamente a todos los @KafkaListener.
     */
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        DeadLetterPublishingRecoverer dlq = new DeadLetterPublishingRecoverer(kafkaTemplate,
                // partición -1 = la elige Kafka (la DLQ no necesita tantas particiones como el original)
                (record, ex) -> new TopicPartition(record.topic() + "-dlq", -1));

        DefaultErrorHandler handler = new DefaultErrorHandler(dlq,
                new FixedBackOff(5000L, FixedBackOff.UNLIMITED_ATTEMPTS));

        handler.addNotRetryableExceptions(
                EventoInvalidoException.class,
                NullPointerException.class,
                IllegalArgumentException.class);
        return handler;
    }
}