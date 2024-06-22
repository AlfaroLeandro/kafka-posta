package com.vinsguru.reactive_kafka_playground.sec15;

public record TransferEvent(
        String key,
        String from,
        String to,
        String amount,
        Runnable acknowledge
) {
}
