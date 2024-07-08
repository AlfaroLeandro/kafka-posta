package com.vinsguru.payment.common.dto;

import com.vinsguru.common.events.payment.PaymentStatus;

import java.util.UUID;

public record PaymentDTO(
        UUID paymentId,
        UUID orderId,
        Integer customerId,
        Integer amount,
        PaymentStatus status
) {
}
