package com.baris.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerSample {
    private static final String bootstrap_servers = "localhost:9092";
    private static final String topicName = "topicName";

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ProducerSample.class);
        final Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        final Producer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 100; i++) {
            String key = "id_" + Integer.toString(i);
            final ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topicName, key, "hello world " + Integer.toString(i));

            logger.info("Key: " + key);

            producer.send(producerRecord, (recordMetadata, e) -> {
                if (e == null) {
                    logger.info("Received new metadata. \n" +
                            "Topic :" + recordMetadata.topic() + "\n" +
                            "Partition : " + recordMetadata.partition() + "\n" +
                            "Offset :" + recordMetadata.offset() + "\n" +
                            "Timestamo :" + recordMetadata.timestamp());
                } else {
                    logger.error("Error while producing", e);
                }
            }).get();
        }
        producer.flush();
        producer.close();
    }
}
