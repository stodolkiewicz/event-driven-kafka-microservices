package com.appsdeveloperblog.ws.products;

import com.appsdeveloperblog.ws.core.ProductCreatedEvent;
import com.appsdeveloperblog.ws.products.rest.CreateProductRestModel;
import com.appsdeveloperblog.ws.products.service.ProductService;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.lang.NonNull;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Testcontainers
@ActiveProfiles("test")
public class ProductsServiceTestContainersIT {
    @Container
    static ConfluentKafkaContainer kafka = new ConfluentKafkaContainer("confluentinc/cp-kafka:7.4.0");

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.producer.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired
    private ProductService productService;

    @Autowired
    ConsumerFactory<String, ProductCreatedEvent> consumerFactory;

    @Value("${product-created-events-topic-name}")
    String topicName;

    private static final String PRODUCT_TITLE = "iPhone 13";
    private static final BigDecimal PRODUCT_PRICE = new BigDecimal(600);
    private static final Integer PRODUCT_QUANTITY = 1;

    @Test
    void testCreateProduct_whenGivenValidProductDetails_successfullySendsKafkaMessage() throws Exception {
        // given
        CreateProductRestModel createProductRestModel = new CreateProductRestModel(PRODUCT_TITLE, PRODUCT_PRICE, PRODUCT_QUANTITY);

        Consumer<String, ProductCreatedEvent> consumer = consumerFactory.createConsumer();
        consumer.subscribe(Collections.singleton(topicName));

        // when
        String productId = productService.createProduct(createProductRestModel);
        ConsumerRecord<String, ProductCreatedEvent> record = KafkaTestUtils.getSingleRecord(consumer, topicName);

        // then
        assertThat(record).isNotNull();
        assertThat(record.key()).isEqualTo(productId);
        assertThat(record.value()).isNotNull();
        assertThat(record.topic()).isEqualTo(topicName);

        ProductCreatedEvent event = record.value();
        assertThat(event.productId()).isEqualTo(productId);
        assertThat(event.title()).isEqualTo("iPhone 13");
        assertThat(event.price()).isEqualTo(new BigDecimal(600));
        assertThat(event.quantity()).isEqualTo(1);

        assertThat(record.headers()).isNotEmpty();
        assertThat(record.headers().headers("messageId")).isNotNull();
    }

    @Test
    void testCreateProducts_whenGivenValidProductDetails_successfullySendsKafkaMessages() throws Exception {
        // given
        CreateProductRestModel createProductRestModel1 = new CreateProductRestModel(PRODUCT_TITLE.concat("a"), PRODUCT_PRICE, PRODUCT_QUANTITY);
        CreateProductRestModel createProductRestModel2 = new CreateProductRestModel(PRODUCT_TITLE.concat("b"), PRODUCT_PRICE, PRODUCT_QUANTITY);
        CreateProductRestModel createProductRestModel3 = new CreateProductRestModel(PRODUCT_TITLE.concat("c"), PRODUCT_PRICE, PRODUCT_QUANTITY);

        Consumer<String, ProductCreatedEvent> consumer = consumerFactory.createConsumer();
        consumer.subscribe(Collections.singleton(topicName));

        // when
        String productId1 = productService.createProduct(createProductRestModel1);
        String productId2 = productService.createProduct(createProductRestModel2);
        String productId3 = productService.createProduct(createProductRestModel3);

        ConsumerRecords<String, ProductCreatedEvent> records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(15));

        // then
        assertThat(records).hasSize(3);

        List<ConsumerRecord<String, ProductCreatedEvent>> recordList = new ArrayList<>();
        records.forEach(recordList::add);

        assertThat(recordList.get(0).key()).isEqualTo(productId1);
        assertThat(recordList.get(1).key()).isEqualTo(productId2);
        assertThat(recordList.get(2).key()).isEqualTo(productId3);
    }

/*
    // Simple in-class bean configuration. Not needed as TestContainersConfig exists.
    @TestConfiguration
    static class TestConfig {

        @Bean
        public ConsumerFactory<String, ProductCreatedEvent> testConsumerFactory() {
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-custom-group");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(JacksonJsonDeserializer.TRUSTED_PACKAGES, "*");

            return new DefaultKafkaConsumerFactory<>(props);
        }

        @Profile("test")
        @Bean
        public NewTopic createTopic() {
            return TopicBuilder.name("product-created-events-topic")
                    .partitions(1)
                    .replicas(1)
                    .build();
        }
    }
 */

}
