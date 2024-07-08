package com.vinsguru.common.processor;

import com.vinsguru.common.events.DomainEvent;
import com.vinsguru.common.events.order.OrderEvent;
import reactor.core.publisher.Mono;


public interface OrderEventProcessor<R extends DomainEvent> extends EventProcessor<OrderEvent, R> {

    @Override
    default Mono<R> process(OrderEvent event) {
        return switch (event) {
            case OrderEvent.Created e -> this.handle(e);
            case OrderEvent.Completed e -> this.handle(e);
            case OrderEvent.Canceled e -> this.handle(e);
        };
    }

    Mono<R> handle(OrderEvent.Created e);
    Mono<R> handle(OrderEvent.Canceled e);
    Mono<R> handle(OrderEvent.Completed e);
}
