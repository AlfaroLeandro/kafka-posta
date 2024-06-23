package com.vinsguru.reactive_kafka_playground.sec12;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    public static void main(String[] args) {

        var options = ReceiverOptions.create(Map.of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                    ConsumerConfig.GROUP_ID_CONFIG, "demo-group",
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                    ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1"
            )).subscription(List.of("order-events"));

        KafkaReceiver.create(options)
                .receive()
                .log()
                .concatMap(KafkaConsumer::process)
                .subscribe();
    }

    private static Mono<Void> process(ReceiverRecord<Object, Object> receiverRecord) {
        return Mono.just(receiverRecord)
                .doOnNext(r -> {
                    var index = ThreadLocalRandom.current().nextInt(1,100);
                    log.info("key: {}, value: {}", r.key(), r.value().toString().toCharArray()[index]); //EXCEPTION AVECES, AVECES NO
                    r.receiverOffset().acknowledge();
                })
                .retryWhen(Retry.fixedDelay(3, Duration.ofSeconds(1))
                        .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> retrySignal.failure()))
                .doOnError(ex -> log.error(ex.getMessage()))
                .doFinally(__ -> receiverRecord.receiverOffset().acknowledge())
                .onErrorComplete() //limpio la señal de error
                .then();
    }
}
