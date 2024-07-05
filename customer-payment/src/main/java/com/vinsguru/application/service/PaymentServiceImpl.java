package com.vinsguru.application.service;

import com.vinsguru.application.entity.Customer;
import com.vinsguru.application.entity.CustomerPayment;
import com.vinsguru.application.mapper.EntityDTOMapper;
import com.vinsguru.application.repository.CustomerRepository;
import com.vinsguru.application.repository.PaymentRepository;
import com.vinsguru.common.dto.PaymentDTO;
import com.vinsguru.common.dto.PaymentProcessRequest;
import com.vinsguru.common.events.payment.PaymentStatus;
import com.vinsguru.common.exception.CustomerNotFoundException;
import com.vinsguru.common.exception.InsufficientBalanceException;
import com.vinsguru.common.service.PaymentService;
import com.vinsguru.util.DuplicateEventValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Service
public class PaymentServiceImpl implements PaymentService {
    private static final Logger log = LoggerFactory.getLogger(PaymentServiceImpl.class);
    private static final Mono<Customer> CUSTOMER_NOT_FOUND = Mono.error(new CustomerNotFoundException());
    private static final Mono<Customer> INSUFFICIENT_BALANCE = Mono.error(new InsufficientBalanceException());

    @Autowired
    private CustomerRepository customerRepository;

    @Autowired
    private PaymentRepository paymentRepository;


    @Override
    @Transactional
    public Mono<PaymentDTO> process(PaymentProcessRequest request) {
        return DuplicateEventValidator.validate(
                this.paymentRepository.existsByOrderId(request.orderId()),
                this.customerRepository.findById(request.customerId())
        )
                .switchIfEmpty(CUSTOMER_NOT_FOUND)
                .filter(c -> c.getBalance() >= request.amount())
                .switchIfEmpty(INSUFFICIENT_BALANCE)
                .flatMap(c -> this.deductPayment(c, request))
                .doOnNext(dto -> log.info("payment processed for {}: ", dto.orderId()));
    }

    private  Mono<PaymentDTO> deductPayment(Customer customer, PaymentProcessRequest request) {
        var customerPayment = EntityDTOMapper.toCustomerPayment(request);
        customerPayment.setStatus(PaymentStatus.DEDUCTED);
        return this.customerRepository.save(customer)
                .then(this.paymentRepository.save(customerPayment))
                .map(EntityDTOMapper::toDto);
    }

    @Override
    @Transactional
    public Mono<PaymentDTO> refund(UUID orderId) {
        return this.paymentRepository.findByOrderIdAndStatus(orderId, PaymentStatus.DEDUCTED)
                .zipWhen(cp -> this.customerRepository.findById(cp.getCustomerId()))
                .flatMap(t -> this.refundPayment(t.getT1(), t.getT2()))
                .doOnNext(dto -> log.info("refund amount {} for {}: ", dto.amount(), dto.orderId()));
    }

    private Mono<PaymentDTO> refundPayment(CustomerPayment customerPayment, Customer customer) {
        customer.setBalance(customer.getBalance() + customerPayment.getAmount());
        customerPayment.setStatus(PaymentStatus.REFUNDED);
        return this.customerRepository.save(customer)
                .then(this.paymentRepository.save(customerPayment))
                .map(EntityDTOMapper::toDto);
    }
}
