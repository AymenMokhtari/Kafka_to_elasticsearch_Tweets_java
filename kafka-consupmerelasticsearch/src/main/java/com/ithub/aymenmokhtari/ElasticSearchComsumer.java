package com.ithub.aymenmokhtari;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchComsumer {

        public static RestHighLevelClient createClient(){

            String hostname = "kafka-save-twitter-1046291186.eu-central-1.bonsaisearch.net";
            String username = "birpi0c87a"; // needed only for bonsai
            String password = "rzf4oevtd1"; // needed only for bonsai


        // credentials provider help supply username and password
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {

                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }
    public static KafkaConsumer<String , String> createConsumer(String topic) {
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";

        Logger logger = LoggerFactory.getLogger(ElasticSearchComsumer.class.getName());
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG , bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG , StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG  , StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG  , groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG , "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG ,"false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG ,"20");


        // create comsumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));

    return consumer;
    }

    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchComsumer.class.getName());

        RestHighLevelClient client = createClient();


    KafkaConsumer<String , String> consumer = createConsumer("twitter_tweets");
        // close the client

        while(true) {
            ConsumerRecords<String , String> records =  consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0
            logger.info("Received " +records.count()+ " records");
            for(ConsumerRecord<String, String> record :records) {
        // 2 strategies  make id
                //  kafka geniric ID

               // String id = record.topic() +"_"+ record.partition()+"_"+ record.offset();
                //twitter feed specific id
                String id = extractIdFromTweet(record.value());
                String jsonString = record.value();
                //where to insert data into ElasticSearch


                //id to make our cnosumer idempotent
                IndexRequest indexRequest = new IndexRequest("twitter" , "tweets" , id ).source(jsonString, XContentType.JSON);
                IndexResponse indexResponse = client.index(indexRequest , RequestOptions.DEFAULT);
                String id_tw = indexResponse.getId();
                logger.info(indexResponse.getId());
                try {
                    Thread.sleep(10); // introduce  a small delay
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }


            }
            logger.info("Committing offsets ...");
            consumer.commitSync();
            logger.info("Offsets have been committed");

            try{
                Thread.sleep(1000);

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


    }
    private static JsonParser jsonParser = new JsonParser();

    private static String extractIdFromTweet(String tweetJson) {
        return jsonParser.parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();



    }
}
