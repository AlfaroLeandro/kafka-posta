package com.vinsguru.common.events.order;

import com.vinsguru.common.events.DomainEvent;
import com.vinsguru.common.events.OrderSaga;

import java.time.Instant;
import java.util.UUID;

public sealed interface OrderEvent extends DomainEvent, OrderSaga {

    public record Created(UUID orderId,
                          Integer productId,
                          Integer customerId,
                          Integer quantity,
                          Integer unitPrice,
                          Integer totalAmount,
                          Instant createdAt) implements OrderEvent {}

    public record Completed(UUID orderId, Instant createdAt) implements OrderEvent {}

    public record Canceled(UUID orderId,
                           String message,
                           Instant createdAt) implements OrderEvent {}



}
