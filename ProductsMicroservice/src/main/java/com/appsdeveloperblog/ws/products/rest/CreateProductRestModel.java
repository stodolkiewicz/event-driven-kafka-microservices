package com.appsdeveloperblog.ws.products.rest;

import java.math.BigDecimal;

public record CreateProductRestModel(String title, BigDecimal price, Integer quantity) {
}
