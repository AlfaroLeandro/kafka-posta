package com.vinsguru.payment.messaging.config;

import com.vinsguru.common.events.order.OrderEvent;
import com.vinsguru.common.events.payment.PaymentEvent;
import com.vinsguru.common.processor.OrderEventProcessor;
import com.vinsguru.util.MessageConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import reactor.core.publisher.Flux;

import java.util.function.Function;

@Configuration
public class OrderEventProcessorConfig {
    private static final Logger log = LoggerFactory.getLogger(OrderEventProcessorConfig.class);

    @Autowired
    private OrderEventProcessor<PaymentEvent> eventProcessor;

    @Bean
    public Function<Flux<Message<OrderEvent>>, Flux<Message<PaymentEvent>>> processor() {
        return messageFlux -> messageFlux.map(MessageConverter::toRecord)
                                .doOnNext(r -> log.info("customer payment received {}", r.message()))
                                .concatMap(r -> this.eventProcessor.process(r.message())
//                                        .retry(2) el mejor lugar para retry o errores
//                                        .onErrorResume()
                                                    .doOnNext(e -> r.acknowledgement().acknowledge()))
                                .map(this::toMessage);

    }

    private Message<PaymentEvent> toMessage(PaymentEvent event) {
        return MessageBuilder.withPayload(event)
                    .setHeader(KafkaHeaders.KEY, event.orderId().toString())
                    .build();
    }

}
