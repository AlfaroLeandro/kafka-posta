package com.vinsguru.reactive_kafka_playground.sec15;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;

public class TransferEventConsumer {
    private static final Logger log = LoggerFactory.getLogger(TransferEventConsumer.class);
    private final KafkaReceiver<String, String> receiver;

    public TransferEventConsumer(KafkaReceiver<String, String> receiver) {
        this.receiver = receiver;
    }

    public Flux

    private TransferEvent toTransferEvent(ReceiverRecord<String, String> record) {
        var arr = record.value().split(",");
        var runnable = record.key().equals("6")? fail(record) : ack(record); //error del programador
        return new TransferEvent(
                record.key(),
                arr[0],
                arr[1],
                arr[2],
                runnable
        );
    }

    private Runnable ack(ReceiverRecord<String, String> record) {
        return () -> record.receiverOffset().acknowledge();
    }

    private Runnable fail(ReceiverRecord<String, String> record) {
        return () -> {throw new RuntimeException("error while ack");};
    }
}
