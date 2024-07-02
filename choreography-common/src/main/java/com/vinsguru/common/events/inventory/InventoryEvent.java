package com.vinsguru.common.events.inventory;

import com.vinsguru.common.events.DomainEvent;
import com.vinsguru.common.events.OrderSaga;

import java.time.Instant;
import java.util.UUID;

public sealed interface InventoryEvent extends DomainEvent, OrderSaga {

    record Deducted(UUID orderId,
                    UUID inventoryId,
                    Integer productId,
                    Integer quantity,
                    Instant createdAt) implements InventoryEvent {}

    record Restored(UUID orderId,
                    UUID inventoryId,
                    Integer productId,
                    Integer quantity,
                    Instant createdAt) implements InventoryEvent {}

    record Declined(UUID orderId,
                    UUID inventoryId,
                    Integer productId,
                    Integer quantity,
                    String message,
                    Instant createdAt) implements InventoryEvent {}
}
