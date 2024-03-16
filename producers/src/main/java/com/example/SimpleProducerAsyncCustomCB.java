package com.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

public class SimpleProducerAsyncCustomCB {

    private static final Logger logger = LoggerFactory.getLogger(SimpleProducerAsyncCustomCB.class);

    public static void main(String[] args) {
        String topicName = "multipart-topic";

        // KafkaProducer configuration settings
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.2:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName() );
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        for (int seq = 0; seq < 20; seq++) {
            // KafkaProducer instance
            KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);

            // ProducerRecord
            ProducerRecord<Integer, String> record = new ProducerRecord<>(topicName, seq, "Hello, Kafka " + seq);

            // produce
            producer.send(record, new CustomCallback(seq));
            producer.flush();
            producer.close();
        }
        logger.info("### message sent ###");
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
