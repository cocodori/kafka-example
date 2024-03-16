package com.example;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomCallback implements Callback {

    private static final Logger logger = LoggerFactory.getLogger(CustomCallback.class);
    private final int sequence;

    public CustomCallback(int sequence) {
        this.sequence = sequence;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {

        if (exception != null) {
            logger.error("### Exception occurred ###", exception);
        } else {
            logger.info("### record metadata received ###\n" +
                    "sequence: {}, partition: {}, offset: {}, timestamp: {}",
                sequence,
                metadata.partition(),
                metadata.offset(),
                metadata.timestamp()
            );
        }
    }
}
