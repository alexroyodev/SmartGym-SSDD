package com.ssdd.smartgym.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic telemetriaTopic() {
        return TopicBuilder.name("kafka-telemetria")
                .partitions(3)
                .replicas(1) // 1 réplica porque solo hay 1 broker local
                .build();
    }

    @Bean
    public NewTopic accesosTopic() {
        return TopicBuilder.name("kafka-accesos")
                .partitions(3)
                .replicas(1)
                .build();
    }

    /** Topic compactado: Kafka conserva solo el último valor por clave ("umbral"). */
    @Bean
    public NewTopic configTopic() {
        return TopicBuilder.name("kafka-config")
                .partitions(1)
                .replicas(1)
                .compact()
                // Valores bajos para que la compactación se aprecie en la demo
                .config(TopicConfig.SEGMENT_MS_CONFIG, "60000")
                .config(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.01")
                .build();
    }

    @Bean
    public NewTopic telemetriaDlqTopic() {
        return TopicBuilder.name("kafka-telemetria-dlq").partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic accesosDlqTopic() {
        return TopicBuilder.name("kafka-accesos-dlq").partitions(1).replicas(1).build();
    }
}