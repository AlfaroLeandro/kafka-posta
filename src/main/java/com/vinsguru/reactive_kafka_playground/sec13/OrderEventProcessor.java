package com.vinsguru.reactive_kafka_playground.sec13;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

public class OrderEventProcessor {

    private final ReactiveDeadLetterTopicProducer<String, String> deadLetterTopicProducer;
    private static final Logger log = LoggerFactory.getLogger(OrderEventProcessor.class);
    public OrderEventProcessor(ReactiveDeadLetterTopicProducer<String, String> deadLetterTopicProducer) {
        this.deadLetterTopicProducer = deadLetterTopicProducer;
    }

    public Mono<Void> process(ReceiverRecord<String, String> record) {
        return Mono.just(record)
                .doOnNext(r -> {
                    if(r.key().endsWith("5"))
                        throw new RuntimeException("processing exception");
                    var index = ThreadLocalRandom.current().nextInt(1,100);
                    log.info("key: {}, value: {}", r.key(), r.value().toString().toCharArray()[index]); //EXCEPTION AVECES, AVECES NO
                    r.receiverOffset().acknowledge();
                })
                .onErrorMap(ex -> new RecordProcessingException(record, ex))
                .transform(this.deadLetterTopicProducer.recordProcessingErrorHandler())
                .then();
    }
}
