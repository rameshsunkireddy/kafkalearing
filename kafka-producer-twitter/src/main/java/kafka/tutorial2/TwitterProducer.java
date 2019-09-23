package kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    public TwitterProducer() {
    }

    public static void main(String[] args) {
        new TwitterProducer().run();

    }

    String consumerKey = "2s1qhhNpL3CIGO6UlOA07URnV";
    String consumerSecret = "ESLYooX3IwWQAYft15xjF8We60YbotKfUFrR63azDTjpFLaLbL";
    String token = "1169507036121329664-axO3felS72UgxtXPHheYu5OvV8UWu9";
    String secret = "xPShs0YaX1eKDEKJR0SkGHIHo3XnAhdkHOru5mUhIW5Z2";

    List<String> terms = Lists.newArrayList("bitcoin", "usa", "politics", "sport", "soccer");


    public void run(){

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);


        Client client = createTwitterClient(msgQueue);
        client.connect();


        KafkaProducer<String, String> producer = createProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {

            logger.info("stopping application ... ");
            client.stop();
            logger.info("stopping twitter client");

            producer.close();
            logger.info("closing kafka producer");

            logger.info("--- DONE ----");

        }
        ));

        while (!client.isDone()) {
            String msg = null;
            try{
                 msg = msgQueue.poll(5, TimeUnit.SECONDS);

            }catch (Exception e){
                e.printStackTrace();
                client.stop();
            }
           if (msg != null){
               logger.info("message:: "+msg);
               producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                   @Override
                   public void onCompletion(RecordMetadata metadata, Exception exception) {
                       if (exception != null){
                           logger.info("something bad happen", exception);
                       }

                   }
               });
           }
        }

        logger.info("completed getting messages");


    }

    public KafkaProducer createProducer(){
        Properties p = new Properties();
        p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        p.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        // idempotent
        p.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        p.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        p.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        p.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // high through put
        p.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        p.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        p.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));


        // producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(p);

        return producer;
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        // twitter auth

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Kafka Client")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();

        return hosebirdClient;

        // create producer

        // push to kafka

    }
}
