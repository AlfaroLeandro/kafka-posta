package com.vinsguru.common.events.payment;

import com.vinsguru.common.events.DomainEvent;
import com.vinsguru.common.events.OrderSaga;

import java.time.Instant;
import java.util.UUID;

public sealed interface PaymentEvent extends DomainEvent, OrderSaga {

    record Deducted(UUID orderId,
                    UUID paymentId,
                    Integer customerId,
                    Integer amount,
                    Instant createdAt) implements PaymentEvent {
    }

    record Refunded(UUID orderId,
                    UUID paymentId,
                    Integer customerId,
                    Integer amount,
                    Instant createdAt) implements PaymentEvent {
    }

    record Declined(UUID orderId,
                    Integer customerId,
                    Integer amount,
                    String message,
                    Instant createdAt) implements PaymentEvent {
    }
}
