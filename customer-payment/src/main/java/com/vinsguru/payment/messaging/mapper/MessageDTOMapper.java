package com.vinsguru.payment.messaging.mapper;

import com.vinsguru.payment.common.dto.PaymentDTO;
import com.vinsguru.payment.common.dto.PaymentProcessRequest;
import com.vinsguru.common.events.order.OrderEvent;
import com.vinsguru.common.events.payment.PaymentEvent;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.function.Function;

public class MessageDTOMapper {

    public static PaymentProcessRequest toPaymentProcessRequest(OrderEvent.Created event) {
        return new PaymentProcessRequest(
            event.customerId(),
            event.orderId(),
            event.totalAmount()
        );
    }

    public static PaymentEvent toPaymentDeductedEvent(PaymentDTO dto) {
        return new PaymentEvent.Deducted(
                dto.paymentId(),
                dto.orderId(),
                dto.amount(),
                dto.customerId(),
                Instant.now()
        );
    }

    public static PaymentEvent toPaymentRefundedEvent(PaymentDTO dto) {
        return new PaymentEvent.Refunded(
                dto.paymentId(),
                dto.orderId(),
                dto.amount(),
                dto.customerId(),
                Instant.now()
        );
    }

    public static Function<Throwable, Mono<PaymentEvent>> toPaymentDeclinedEvent(OrderEvent.Created event) {
        return ex -> Mono.fromSupplier(() -> new PaymentEvent.Declined(
                                                event.orderId(),
                                                event.totalAmount(),
                                                event.customerId(),
                                                ex.getMessage(),
                                                Instant.now()));
    }
}
