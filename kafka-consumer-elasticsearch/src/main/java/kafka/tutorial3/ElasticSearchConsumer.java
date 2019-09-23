package kafka.tutorial3;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

        RestHighLevelClient client = createClient();



        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            Integer recCount = records.count();
            logger.info("records count: "+ records.count());

            BulkRequest bulkRequest = new BulkRequest();
            for (ConsumerRecord<String, String> rec: records){

                try{
                    logger.info("key: "+rec.key() +", val: "+rec.value() + ", partition: "+rec.partition() + " ,offset: "+rec.offset());

//                String gen_id = rec.topic() + "_" + rec.partition() + "_" + rec.offset();
                    String gen_id =extractIdFromTweet(rec.value());
                    IndexRequest request = new IndexRequest("twitter_tweets").id(gen_id);
                    bulkRequest.add(request);

                    request.source("{\"tweet\":\"hello " + rec.offset() +"\"}", XContentType.JSON);

                } catch (Exception e){
                    logger.warn("warning bad data------");
                }


//                IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);

//                String id = indexResponse.getId();

//                logger.info("id:: "+id);

            }

            if (recCount > 0){
                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                logger.info("committing offsets");
                consumer.commitSync();
                logger.info("-- offsets committed");

                try{
                    Thread.sleep(1000);
                } catch (Exception e){
                    e.printStackTrace();
                }
            }


        }

//        client.close();
    }

    private static  com.google.gson.JsonParser g_parser  = new JsonParser();
    static  String extractIdFromTweet(String tweet){

       return  g_parser.parse(tweet).getAsJsonObject().get("id_str").getAsString();
    }

    static KafkaConsumer<String, String> createConsumer(String topic){
        String bootstrapSevers = "127.0.0.1:9092";
        String groupId = "elastic_search_app";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapSevers);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }

    static RestHighLevelClient createClient(){

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http"),
                        new HttpHost("localhost", 9201, "http")));

        return client;

    }


}
