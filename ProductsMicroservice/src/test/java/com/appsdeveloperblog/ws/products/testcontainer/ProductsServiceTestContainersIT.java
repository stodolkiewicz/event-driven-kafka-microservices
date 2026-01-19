package com.appsdeveloperblog.ws.products.testcontainer;

import com.appsdeveloperblog.ws.core.ProductCreatedEvent;
import com.appsdeveloperblog.ws.products.rest.CreateProductRestModel;
import com.appsdeveloperblog.ws.products.service.ProductService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.assertj.core.api.Assertions.assertThat;

public class ProductsServiceTestContainersIT extends KafkaITBase {
    @Autowired
    private ProductService productService;

    @Value("${product-created-events-topic-name}")
    String topicName;

    private ConcurrentMessageListenerContainer<String, ProductCreatedEvent> container;
    private BlockingQueue<ConsumerRecord<String, ProductCreatedEvent>> records = new LinkedBlockingQueue<>();

    @BeforeAll
    void setup() {
        DefaultKafkaConsumerFactory<String, Object> consumerFactory = new DefaultKafkaConsumerFactory<>(
                KafkaTestContainersITUtils.getConsumerProperties(kafka, environment)
        );
        container = new ConcurrentMessageListenerContainer<>(
                consumerFactory,
                new ContainerProperties(environment.getProperty("product-created-events-topic-name"))
        );

        // register message listener, which will add incoming records to records BlockingQueue
        container.setupMessageListener((MessageListener<String, ProductCreatedEvent>) records::add);

        container.start();

        // wait for assignment of exactly 1 partition
        ContainerTestUtils.waitForAssignment(container, 1);
    }

    private static final String PRODUCT_TITLE = "iPhone 13";
    private static final BigDecimal PRODUCT_PRICE = new BigDecimal(600);
    private static final Integer PRODUCT_QUANTITY = 1;

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
}

/*


CURRENT SITUATION (manual management):

    1. Class loading - loading the test class
    2. Static field initialization - kafka = new ConfluentKafkaContainer(...)
    3. Static block execution - kafka.start() ← KAFKA IS RUNNING
    4. @DynamicPropertySource - Spring calls overrideProperties(), Kafka already ready
    5. Spring Context creation - building ApplicationContext with properties
    6. @BeforeAll setup() - creating consumer
    7. @Test methods

    ---
  WITH ANNOTATIONS (@Testcontainers + @Container):

    1. Class loading - loading the test class
    2. Static field initialization - kafka = new ConfluentKafkaContainer(...) (but does NOT
  start!)
    3. JUnit extension setup - JUnit prepares lifecycle
    4. @DynamicPropertySource - Spring tries to call kafka.getBootstrapServers() ← 💥 KAFKA
  NOT RUNNING YET
    5. @Container start - only now JUnit starts the container
    6. Spring Context creation - might be too late, properties already fetched
    7. @BeforeAll setup()
    8. @Test methods

*/
