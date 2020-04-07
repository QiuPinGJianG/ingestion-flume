package com.it21learning.ingestion.kafka.streaming;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class UserFriendsStream {
    public static void main(String[] args) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG,"user-friends-streamming");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"Sandbox-hdp.hortonworks.com:6667");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG,"Sandbox-hdp.hortonworks.com:2181");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.STATE_DIR_CONFIG,"/tmp");
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);

        //building Kafka Streams Model
        // 构建Kafka流模型
        KStreamBuilder kStreamBuilder = new KStreamBuilder();
        //the source of the streaming analysis is the topic with country messages
        // 流式分析的来源是包含国家信息的主题
        KStream<String, String> stream = kStreamBuilder.stream(Serdes.String(), Serdes.String(), "user_friends_raw");
        //transform
        KStream<String, String> result = stream.flatMap((k, v) -> transform(k, v)).filter((k, v) -> v != null && v.length() > 0);
        //publish
        result.to(Serdes.String(), Serdes.String(), "user_friends");
        //create an instance of StreamsConfig from the Properties instance
        StreamsConfig config = new StreamsConfig(props);

        (new KafkaStreams(kStreamBuilder, config)).start();

    }
    // 判断有没有header
    protected static Boolean isHeader(String[] fields) {
        //check
        return (isValid(fields) && fields[0].equals("user") && fields[1].equals("friends"));
    }

    // 判断有效性
    protected static Boolean isValid(String[] fields) {
        //check
        return (fields.length > 1);
    }
    //transform the record data
    private static Iterable<KeyValue<String, String>> transform(String key, String value) {
        //create
        List<KeyValue<String, String>> items = new ArrayList<KeyValue<String, String>>();
        //split
        String[] fields = value.split(",",  -1); // 保留null值 and 遇到无值的空间继续切分
        //check
        if ( isHeader(fields) || !isValid(fields) ) {
            //add
            items.add(new KeyValue<>(key, ""));
        } // 丢弃行为
        else {
            //loop
            for (String[] vs: transform(fields)) {
                //add
                items.add(new KeyValue<>(key, String.join(",", vs)));
            }
        }
        return items;
    }

    protected static List<String[]> transform(String[] fields) {
        //put collection
        List<String[]> results = new ArrayList<String[]>();
        //user
        String user = fields[0];
        //friends
        String[] friends = fields[1].split(" ");
        //check
        if ( friends != null && friends.length > 0 ) {
            //loop
            for ( String friend : friends ) {
                //add
                results.add(new String[] { user, friend });
            }
        }
        return results;
    }
}
