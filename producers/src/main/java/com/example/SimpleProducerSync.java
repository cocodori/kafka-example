package com.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SimpleProducerSync {

    private static final Logger logger = LoggerFactory.getLogger(SimpleProducerSync.class);

    public static void main(String[] args) {
        String topicName = "simple-topic";

        // KafkaProducer configuration settings
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.2:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer instance
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // ProducerRecord
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "Hello, Kafka !");
        try {
            RecordMetadata recordMetadata = producer.send(record).get();
            logger.info("### record metadata received ###\n" +
                "partition:" + recordMetadata.partition() + "\n" +
                "offset:" + recordMetadata.offset() + "\n" +
                "timestamp:" + recordMetadata.timestamp()
            );
            producer.flush();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } finally {
            producer.close();
        }
    }
}
