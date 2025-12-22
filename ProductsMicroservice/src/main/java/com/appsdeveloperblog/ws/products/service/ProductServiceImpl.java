package com.appsdeveloperblog.ws.products.service;

import com.appsdeveloperblog.ws.core.ProductCreatedEvent;
import com.appsdeveloperblog.ws.products.rest.CreateProductRestModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class ProductServiceImpl implements ProductService {
    KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate;
    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    public ProductServiceImpl(KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public String createProduct(CreateProductRestModel productRestModel) throws Exception {
        String productId = UUID.randomUUID().toString();

        // TODO: Persist ProductDetails into database table before publishing an Event

        ProductCreatedEvent productCreatedEvent = new ProductCreatedEvent(
                productId,
                productRestModel.title(),
                productRestModel.price(),
                productRestModel.quantity()
        );

        // synchronous
        SendResult<String, ProductCreatedEvent> result =
                kafkaTemplate.send("product-created-events-topic", productId, productCreatedEvent).get();
        LOGGER.info("Topic: " + result.getRecordMetadata().topic());
        LOGGER.info("Partition: " + result.getRecordMetadata().partition());
        LOGGER.info("Offset: " + result.getRecordMetadata().offset());

        LOGGER.info("***** Returning product id");

        return productId;
    }
}
