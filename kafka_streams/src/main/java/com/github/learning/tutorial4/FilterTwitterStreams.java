package com.github.learning.tutorial4;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class FilterTwitterStreams {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> inputTopic =  streamsBuilder.stream("twitter_tweets");

        KStream<String, String> filteredStream = inputTopic.filter(
                (k,tweet) -> extractUserFollowerInTweets(tweet) > 10000

        );

        filteredStream.to("important_tweets");

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        kafkaStreams.start();
    }

    private static  com.google.gson.JsonParser g_parser  = new JsonParser();
    static  Integer extractUserFollowerInTweets(String tweet){

        try{
            return g_parser.parse(tweet).getAsJsonObject().get("user").getAsJsonObject().get("followers_count").getAsInt();
        } catch (NullPointerException e){
            return 0;
        }

    }


}
