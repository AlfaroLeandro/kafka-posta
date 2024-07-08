package com.vinsguru.payment.common.service;

import com.vinsguru.payment.common.dto.PaymentDTO;
import com.vinsguru.payment.common.dto.PaymentProcessRequest;
import reactor.core.publisher.Mono;

import java.util.UUID;

public interface PaymentService {

    Mono<PaymentDTO> process(PaymentProcessRequest request);

    Mono<PaymentDTO> refund(UUID orderId);

}
