package com.vinsguru.common.events.shipping;

import com.vinsguru.common.events.DomainEvent;
import com.vinsguru.common.events.OrderSaga;

import java.time.Instant;
import java.util.UUID;

public sealed interface ShippingEvent extends DomainEvent, OrderSaga {

    record Scheduled(UUID orderId,
                     UUID shipmentId,
                     Instant expectedDelivery,
                     Instant createdAt) implements ShippingEvent {}
}
