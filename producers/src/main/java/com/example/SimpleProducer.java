package com.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class SimpleProducer {

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
        producer.send(record);
        producer.flush();
        producer.close();
    }
}
