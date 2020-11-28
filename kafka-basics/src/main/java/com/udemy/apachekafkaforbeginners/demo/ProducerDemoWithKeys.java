package com.udemy.apachekafkaforbeginners.demo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {
    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

        String bootstrapServers = "localhost:9092";

        // set producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // send data
        for (int i = 0; i < 20; i++) {

            String topic = "first_topic";
            String value = "message " + i;
            String key = "id_" + i;

            // create producer record
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            // send record with callback
            producer.send(record, (recordMetadata, e) -> {
                // is executed every time a record is successfully sent or an exception is thrown
                if (e == null) {
                    // record was sent successfully
                    logger.info("Received new metadata: \n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp());
                } else {
                    logger.error("Error while producing", e);
                }
            });

        }

        // flush records
        producer.flush();

        // flush and close producer
        producer.close();
    }
}
