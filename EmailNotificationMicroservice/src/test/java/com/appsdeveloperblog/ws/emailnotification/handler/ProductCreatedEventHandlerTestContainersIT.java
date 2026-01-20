package com.appsdeveloperblog.ws.emailnotification.handler;

import com.appsdeveloperblog.ws.core.ProductCreatedEvent;
import com.appsdeveloperblog.ws.emailnotification.io.ProcessedEventEntity;
import com.appsdeveloperblog.ws.emailnotification.io.ProcessedEventRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.http.*;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.web.client.RestTemplate;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

@SpringBootTest
@ActiveProfiles("test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ProductCreatedEventHandlerTestContainersIT {
    static ConfluentKafkaContainer kafka = new ConfluentKafkaContainer("confluentinc/cp-kafka:7.4.0");

    static {
        kafka.start();
    }

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.consumer.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @MockitoBean
    ProcessedEventRepository processedEventRepository;

    @MockitoBean
    RestTemplate restTemplate;

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    @MockitoSpyBean
    ProductCreatedEventHandler productCreatedEventHandler;

    @Test
    void testProductCreatedEventHandler_onProductCreated_HandlesEvent() throws ExecutionException, InterruptedException {
        // given
        ProductCreatedEvent productCreatedEvent = new ProductCreatedEvent(
                UUID.randomUUID().toString(),
                "Test product",
                new BigDecimal(10),
                1
        );

        String messageId = UUID.randomUUID().toString();
        String messageKey = productCreatedEvent.productId();

        ProducerRecord<String, Object> record = new ProducerRecord<>(
                "product-created-events-topic",
                messageKey,
                productCreatedEvent
        );

        record.headers().add("messageId", messageId.getBytes());
        record.headers().add(KafkaHeaders.RECEIVED_KEY, messageKey.getBytes());

        when(processedEventRepository.findByMessageId(anyString())).thenReturn(null);
        when(processedEventRepository.save(any(ProcessedEventEntity.class))).thenReturn(null);

        String responseBody = "{\"key\":\"value\"}";
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<String> responseEntity = new ResponseEntity<>(responseBody, headers, HttpStatus.OK);

        when(
                restTemplate.exchange(
                        any(String.class),
                        any(HttpMethod.class),
                        isNull(),
                        eq(String.class)
                )
        ).thenReturn(responseEntity);

        // when
        kafkaTemplate.send(record).get();

        // then
        ArgumentCaptor<String> messageIdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> messageKeyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ProductCreatedEvent> eventCaptor = ArgumentCaptor.forClass(ProductCreatedEvent.class);

        await()
                .pollInterval(Duration.ofSeconds(3))
                .atMost(10, SECONDS)
                .untilAsserted(() -> {
                    verify(
                            productCreatedEventHandler,
                            timeout(5000).times(1)
                    ).handle(
                            eventCaptor.capture(),
                            messageIdCaptor.capture(),
                            messageKeyCaptor.capture()
                    );

                    assertEquals(messageId, messageIdCaptor.getValue());
                    assertEquals(messageKey, messageKeyCaptor.getValue());
                    assertEquals(productCreatedEvent.productId(), eventCaptor.getValue().productId());

                    verify(processedEventRepository, times(1)).findByMessageId(messageId);
                    verify(processedEventRepository, times(1)).save(any(ProcessedEventEntity.class));

                    verify(restTemplate, times(1)).exchange(
                            eq("http://localhost:8082/response/200"),
                            eq(HttpMethod.GET),
                            isNull(),
                            eq(String.class)
                    );
                });
    }
}
