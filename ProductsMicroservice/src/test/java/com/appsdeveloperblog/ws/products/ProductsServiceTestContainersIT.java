package com.appsdeveloperblog.ws.products;

import com.appsdeveloperblog.ws.core.ProductCreatedEvent;
import com.appsdeveloperblog.ws.products.rest.CreateProductRestModel;
import com.appsdeveloperblog.ws.products.service.ProductService;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.*;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import java.math.BigDecimal;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ProductsServiceTestContainersIT {
    static ConfluentKafkaContainer kafka = new ConfluentKafkaContainer("confluentinc/cp-kafka:7.4.0");

//    // 4. DODAJ BLOK STATYCZNY - Start ręczny
    static {
        kafka.start();
        // Dzięki temu kontener na 100% działa, zanim Spring spróbuje odczytać 'getBootstrapServers'
    }

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.producer.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);

//        kafka.start();
    }

    @Autowired
    private ProductService productService;

    @Value("${product-created-events-topic-name}")
    String topicName;

    private static final String PRODUCT_TITLE = "iPhone 13";
    private static final BigDecimal PRODUCT_PRICE = new BigDecimal(600);
    private static final Integer PRODUCT_QUANTITY = 1;

    @Autowired
    Environment environment;

    private static ConcurrentMessageListenerContainer<String, ProductCreatedEvent> container;
    private static BlockingQueue<ConsumerRecord<String, ProductCreatedEvent>> records;

    @BeforeAll
    void setup() {
        DefaultKafkaConsumerFactory<String, Object> consumerFactory = new DefaultKafkaConsumerFactory<>(getConsumerProperties());
        ContainerProperties containerProperties = new ContainerProperties(environment.getProperty("product-created-events-topic-name"));
        container = new ConcurrentMessageListenerContainer<>(consumerFactory, containerProperties);
        records = new LinkedBlockingQueue<>();
        container.setupMessageListener((MessageListener<String, ProductCreatedEvent>) records::add);
        container.start();
        ContainerTestUtils.waitForAssignment(container, 1);
    }

    @Test
    void testCreateProduct_whenGivenValidProductDetails_successfullySendsKafkaMessage() throws Exception {
        // given
        CreateProductRestModel createProductRestModel = new CreateProductRestModel(PRODUCT_TITLE, PRODUCT_PRICE, PRODUCT_QUANTITY);

        // when
        String productId = productService.createProduct(createProductRestModel);
        ConsumerRecord<String, ProductCreatedEvent> record = records.poll(10, java.util.concurrent.TimeUnit.SECONDS);

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

        // when
        String productId1 = productService.createProduct(createProductRestModel1);
        String productId2 = productService.createProduct(createProductRestModel2);
        String productId3 = productService.createProduct(createProductRestModel3);


        List<ConsumerRecord<String, ProductCreatedEvent>> receivedRecords = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            ConsumerRecord<String, ProductCreatedEvent> record = records.poll(10, java.util.concurrent.TimeUnit.SECONDS);
            if (record != null) {
                receivedRecords.add(record);
            }
        }

        assertThat(receivedRecords).hasSize(3);
        assertThat(receivedRecords.get(0).key()).isEqualTo(productId1);
        assertThat(receivedRecords.get(1).key()).isEqualTo(productId2);
        assertThat(receivedRecords.get(2).key()).isEqualTo(productId3);
    }

    private Map<String, Object> getConsumerProperties() {
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

}
