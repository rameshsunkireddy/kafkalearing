package kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());

        Properties props = new Properties();

        String topic = "first_topic";
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my_java_app_2");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        // assign
        TopicPartition partToRead = new TopicPartition(topic, 0);
        long offsetToRead = 15L;
        consumer.assign(Arrays.asList(partToRead));


        // seek
        consumer.seek(partToRead, offsetToRead);
        int noOfRecTORead = 5;
        boolean keepOnRead = true;
        int noOfRecReadSoFar = 0;


        while(keepOnRead){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> rec: records){
                logger.info("key: "+rec.key() +", val: "+rec.value() + ", partition: "+rec.partition() + " ,offset: "+rec.offset());

                noOfRecReadSoFar += 1;
                if (noOfRecReadSoFar >= noOfRecTORead){
                    keepOnRead = false;
                    break;

                }
            }




        }


    }

}






