package com.ssdd.smartgym.config;

import org.apache.kafka.clients.admin.NewTopic;
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
}
