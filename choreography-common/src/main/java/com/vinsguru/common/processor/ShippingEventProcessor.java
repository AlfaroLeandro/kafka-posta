package com.vinsguru.common.processor;

import com.vinsguru.common.events.DomainEvent;
import com.vinsguru.common.events.shipping.ShippingEvent;
import reactor.core.publisher.Mono;

public interface ShippingEventProcessor<R extends DomainEvent> extends EventProcessor<ShippingEvent, R> {

    @Override
    default Mono<R> process(ShippingEvent event) {
        return switch(event) {
            case ShippingEvent.Scheduled e -> this.handle(e);
        };
    }

    Mono<R> handle(ShippingEvent.Scheduled e);
}
