package com.vinsguru.reactive_kafka_playground.sec09;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.util.Map;

public class KafkaProducer {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);

    public static void main(String[] args) {

        var options = SenderOptions.<String,String>create(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
//                ProducerConfig.GROUP_ID_CONFIG, "inventory-service-group",
//                ProducerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
//                ProducerConfig.GROUP_INSTANCE_ID_CONFIG, "1"
        ));

        var flux = Flux.range(1,10).map(KafkaProducer::createSenderRecord);

        var sender = KafkaSender.<String, String>create(options);
                sender.send(flux)
                .doOnNext(r -> log.info("correlation id: {}", r.correlationMetadata()))
                .doOnComplete(sender::close) //cierra la conexion con kafka
                .subscribe();
    }

    public static SenderRecord<String, String, String> createSenderRecord(Integer i) {
        var headers = new RecordHeaders();
        headers.add("client-id", "some-client".getBytes());
        headers.add("tracing-id", "123".getBytes());
        var pr = new ProducerRecord<>("order-events", null, i.toString(), "order-" + i, headers);
        return SenderRecord.create(pr, pr.key());
    }
}
