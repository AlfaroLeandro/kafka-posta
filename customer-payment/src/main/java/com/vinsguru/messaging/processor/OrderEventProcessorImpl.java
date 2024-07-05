package com.vinsguru.messaging.processor;

import com.vinsguru.common.events.order.OrderEvent;
import com.vinsguru.common.events.payment.PaymentEvent;
import com.vinsguru.common.exception.CustomerNotFoundException;
import com.vinsguru.common.exception.EventAlreadyProcessedException;
import com.vinsguru.common.exception.InsufficientBalanceException;
import com.vinsguru.common.processor.OrderEventProcessor;
import com.vinsguru.common.service.PaymentService;
import com.vinsguru.messaging.mapper.MessageDTOMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.function.UnaryOperator;

@Service
public class OrderEventProcessorImpl implements OrderEventProcessor<PaymentEvent> {
    private static final Logger log = LoggerFactory.getLogger(OrderEventProcessorImpl.class);
    @Autowired
    private PaymentService service;

    @Override
    public Mono<PaymentEvent> handle(OrderEvent.Created e) {
        return this.service.process(MessageDTOMapper.toPaymentProcessRequest(e))
                .map(MessageDTOMapper::toPaymentDeductedEvent)
                .doOnNext(e1 -> log.info("Payment processed: {}", e1))
                .transform(exceptionHandler(e));
    }

    @Override
    public Mono<PaymentEvent> handle(OrderEvent.Canceled e) {
        return this.service.refund(e.orderId());
    }

    @Override
    public Mono<PaymentEvent> handle(OrderEvent.Completed e) {
        return null;
    }

    private UnaryOperator<Mono<PaymentEvent>> exceptionHandler(OrderEvent.Created event) {
        return mono -> mono.onErrorResume(EventAlreadyProcessedException.class, e -> Mono.empty())
                            .onErrorResume(CustomerNotFoundException.class , MessageDTOMapper.toPaymentDeclinedEvent(event))
                            .onErrorResume(InsufficientBalanceException.class , MessageDTOMapper.toPaymentDeclinedEvent(event));
    }
}
