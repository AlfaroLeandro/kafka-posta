package com.vinsguru.payment.common.dto;

import java.util.UUID;

public record PaymentProcessRequest(
        Integer customerId,
        UUID orderId,
        Integer amount

) {
}
