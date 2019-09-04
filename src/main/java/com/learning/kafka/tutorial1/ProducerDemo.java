package com.learning.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerDemo.class);

        System.out.println("Hello");

        // properties
        Properties p = new Properties();
        p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        p.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(p);


        for (int i=0 ; i<10; i++){
            ProducerRecord<String, String> rec = new ProducerRecord<String, String>("first_topic", "hello "+Integer.toString(i));

            producer.send(rec, new Callback() {
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                            if (e ==null) {
                                logger.info("::Record Meta Data::\n");

                                logger.info("Topic: " + recordMetadata.topic());
                                logger.info("Offset: " + recordMetadata.offset());
                                logger.info("Time: " + recordMetadata.timestamp());
                            }
                            else {
                                logger.info("ERROR while producing");
                            }
                        }
                    }

            );

        }

        producer.flush();
        producer.close();


    }
}
