# Idempotent Producer

`spring.kafka.producer.properties.enable.idempotence` jest domyślnie `true`.

Ale żeby działało, potrzebuje 3 właściwości:
- `spring.kafka.producer.properties.acks=all`
- `spring.kafka.producer.properties.retries > 0`
- `spring.kafka.producer.properties.max.in.flight.requests.per.connection <= 5`

Dlatego lepiej jawnie ustawić:
```
spring.kafka.producer.properties.enable.idempotence=true
```

Bo wtedy jak się przypadkiem zmieni którąś z wymaganych właściwości to aplikacja rzuci błąd.