package com.vinsguru.payment.application.mapper;

import com.vinsguru.payment.application.entity.CustomerPayment;
import com.vinsguru.payment.common.dto.PaymentDTO;
import com.vinsguru.payment.common.dto.PaymentProcessRequest;

public class EntityDTOMapper {

    public static CustomerPayment toCustomerPayment(PaymentProcessRequest request) {
        var p = new CustomerPayment();
        p.setCustomerId(request.customerId());
        p.setOrderId(request.orderId());
        p.setAmount(request.amount());
        return p;
    }

    public static PaymentDTO toDto(CustomerPayment payment) {
        return new PaymentDTO(
            payment.getPaymentId(),
            payment.getOrderId(),
            payment.getCustomerId(),
            payment.getAmount(),
            payment.getStatus()
        );
    }
}
