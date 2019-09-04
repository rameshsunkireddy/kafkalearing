package com.learning.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoThread {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoThread.class.getName());
        new ConsumerDemoThread().run();

    }

    private ConsumerDemoThread(){}

    private void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemoThread.class.getName());
        String bootstrapSevers = "127.0.0.1:9092";
        String groupId = "my_consumer_thread_app";
        String topic = "first_topic";

        CountDownLatch latch = new CountDownLatch(1);

        Runnable myConsumerThread = new ConsumerThread(bootstrapSevers, groupId, topic, latch);

        Thread myThread = new Thread(myConsumerThread);
        myThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {

            logger.info("caught shutdown hook");
            ((ConsumerThread)  myConsumerThread).shutdown();
            try{
                latch.await();
            } catch (InterruptedException e){
               e.printStackTrace();

            }
            logger.info("application has exited");

        }
        ));

        try{
            latch.await();
        } catch (InterruptedException e){
            logger.info("app interrupted");

        } finally {
            logger.info("closing app");
        }

    }
//
    public class ConsumerThread implements Runnable {
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());


        public ConsumerThread(String bootstrapServers, String groupId, String topic, CountDownLatch latch){
            Properties props = new Properties();

            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumer = new KafkaConsumer<String, String>(props);

            consumer.subscribe(Arrays.asList(topic));


            this.latch = latch;

        }
        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                    for (ConsumerRecord<String, String> rec : records) {

                        logger.info("key: " + rec.key() + ", val: " + rec.value() + ", partition: " + rec.partition() + " ,offset: " + rec.offset());
                    }


                }
            } catch (Exception e){
                logger.info("got shutdown signal");
            } finally {
                // done with consumer
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown(){

        }
    }
}






