package com.vinsguru.common.processor;

import com.vinsguru.common.events.DomainEvent;
import com.vinsguru.common.events.inventory.InventoryEvent;
import reactor.core.publisher.Mono;

public interface InventoryEventProcessor <R extends DomainEvent> extends  EventProcessor<InventoryEvent, R> {

    @Override
    default Mono<R> process(InventoryEvent event) {
        return switch (event) {
            case InventoryEvent.Declined e -> this.handle(e);
            case InventoryEvent.Restored e -> this.handle(e);
            case InventoryEvent.Deducted e -> this.handle(e);
        };
    }

    Mono<R> handle(InventoryEvent.Declined e);
    Mono<R> handle(InventoryEvent.Restored e);
    Mono<R> handle(InventoryEvent.Deducted e);
}
