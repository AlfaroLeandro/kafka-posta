package com.vinsguru.common.service;

import com.vinsguru.common.dto.PaymentDTO;
import com.vinsguru.common.dto.PaymentProcessRequest;
import reactor.core.publisher.Mono;

import java.util.UUID;

public interface PaymentService {

    Mono<PaymentDTO> process(PaymentProcessRequest request);

    Mono<PaymentDTO> refund(UUID orderId);

}
