package io.kafka.opensearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchWikimediaConsumer {

    private static  Logger log = LoggerFactory.getLogger(OpenSearchWikimediaConsumer.class.getSimpleName());

    public static RestHighLevelClient createOpenSearchClient(){
        String connString = "https://9kltoinju0:oucrtq8p60@project-cluster-" +
                "3888365748.eu-central-1.bonsaisearch.net:443";


        RestHighLevelClient restHighLevelClient ;
        URI connUri = URI.create(connString);

        String userInfo = connUri.getUserInfo();
        log.info("UserInfo" +userInfo);
        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(),
                    connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer(){

        String boostrapServers = "127.0.0.1:9092";
        String groupId = "consumer-opensearch-demo";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");


        // create consumer
        return new KafkaConsumer<>(properties);

    }

    private static String extractId(String json){
        // gson library
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }



    public static void main(String[] args) throws IOException {
        // first create the open search client
        Logger log = LoggerFactory.getLogger(OpenSearchWikimediaConsumer.class.getSimpleName());
        RestHighLevelClient openSearchClient = createOpenSearchClient();
        // create our kafka Client
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        // we need to create the index on openSearch if it does not exist already
        try(openSearchClient;consumer) {
            boolean indexExists = openSearchClient.indices().
                    exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

            if(!indexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("The wikimedia Index has been created");
            }
            else{
                log.info("The wikimedia already exists");
            }

        consumer.subscribe(Collections.singleton("wikimedia.recentchange"));


        while(true){

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));


            int recordCount = records.count();
            log.info("Received"+ recordCount+"recorders");

            for(ConsumerRecord<String,String> record: records){


             try{
                 IndexRequest indexRequest = new IndexRequest("wikimedia")
                         .source(record.value(), XContentType.JSON);
                 IndexResponse response =openSearchClient.index(indexRequest,RequestOptions.DEFAULT);
                 log.info(response .getId()+ "data is inserting 1 record");
             }
             catch (Exception e){

             }
            }


        }}


        // main code logic

        // close things

    }

}
