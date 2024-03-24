package com.example;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class PizzaProducerCustomPartitioner {

    private static final Logger logger = LoggerFactory.getLogger(PizzaProducerCustomPartitioner.class);

    public static void sendMessage(
        KafkaProducer<String, String> producer,
        String topicName,
        int iterationCount,
        int interIntervalMillis,
        int intervalMillis,
        int intervalCount,
        boolean sync
    ) {
        PizzaMessage pizzaMessage = new PizzaMessage();
        int iterationSequence = 0;
        long seed = 2022;
        Random random = new Random(seed);
        Faker faker = Faker.instance(random);

        while (iterationSequence++ != iterationCount) {
            Map<String, String> messages = pizzaMessage.produceMessage(faker, random, iterationSequence);
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, messages.get("key"), messages.get("message"));
            sendMessage(producer, record, messages, sync);

            if ((intervalCount > 0) && (iterationSequence % intervalCount == 0)) {
                try {
                    logger.info("### intervalCount: {}, intervalMillis: {}", intervalCount, intervalMillis);
                    Thread.sleep(intervalMillis);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            if (interIntervalMillis > 0) {
                try {
                    logger.info("### interIntervalMillis: {}", interIntervalMillis);
                    Thread.sleep(interIntervalMillis);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public static void sendMessage(
        KafkaProducer<String, String> producer,
        ProducerRecord<String, String> record,
        Map<String, String> messages,
        boolean sync
    ) {
        for (int seq = 0; seq < 20; seq++) {
            if (!sync) {
                // produce
                producer.send(record, (metadata, exception) -> {
                    logger.info("async message: {}, partition: {}, offset: {}", messages.get("key"), metadata.partition(), metadata.offset());
                });
            } else {
                try {
                    RecordMetadata metadata = producer.send(record).get();
                    logger.info("sync message: {}, partition: {}, offset: {}", messages.get("key"), metadata.partition(), metadata.offset());
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public static void main(String[] args) {
        String topicName = "pizza-topic-partitioner";

        // KafkaProducer configuration settings
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.2:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
        props.setProperty("custom.specialKey", "P001");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        sendMessage(
            producer,
            topicName,
            -1,
             100,
            0,
            0,
            true
        );

    }
}
