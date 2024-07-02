package com.vinsguru.common.processor;

import com.vinsguru.common.events.DomainEvent;
import com.vinsguru.common.events.payment.PaymentEvent;
import reactor.core.publisher.Mono;

public interface PaymentEventProcessor <R extends DomainEvent> extends EventProcessor<PaymentEvent, R> {
    @Override
    default Mono<R> process(PaymentEvent event) {
        return switch (event) {
            case PaymentEvent.Declined e -> this.handle(e);
            case PaymentEvent.Deducted e -> this.handle(e);
            case PaymentEvent.Refunded e -> this.handle(e);
        };
    }

    Mono<R> handle(PaymentEvent.Deducted e);
    Mono<R> handle(PaymentEvent.Declined e);
    Mono<R> handle(PaymentEvent.Refunded e);
}
