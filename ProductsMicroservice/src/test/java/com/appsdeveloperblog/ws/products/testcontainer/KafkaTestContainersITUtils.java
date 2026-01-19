package com.appsdeveloperblog.ws.products.testcontainer;

import com.appsdeveloperblog.ws.core.ProductCreatedEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import java.util.Map;

public class KafkaTestContainersITUtils {
    private static Map<String, Object> getConsumerProperties(ConfluentKafkaContainer kafka, Environment environment) {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class,
                ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JacksonJsonDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, environment.getProperty("spring.kafka.consumer.group-id"),
                JacksonJsonDeserializer.TRUSTED_PACKAGES, environment.getProperty("spring.kafka.consumer.properties.spring.json.trusted.packages"),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, environment.getProperty("spring.kafka.consumer.auto-offset-reset")
        );
    }

    public static <K,V> ConcurrentMessageListenerContainer<K,V> createContainer(ConfluentKafkaContainer kafka, Environment environment, String topicName) {
        DefaultKafkaConsumerFactory<K, V> consumerFactory = new DefaultKafkaConsumerFactory<>(
                KafkaTestContainersITUtils.getConsumerProperties(kafka, environment)
        );
        ConcurrentMessageListenerContainer<K, V> container = new ConcurrentMessageListenerContainer<>(
                consumerFactory,
                new ContainerProperties(topicName)
        );

        return container;
    }
}