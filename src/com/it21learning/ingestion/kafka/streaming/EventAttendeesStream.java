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

public class EventAttendeesStream {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,"event-attendees-streamming");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"Sandbox-hdp.hortonworks.com:6667");


        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG,"Sandbox-hdp.hortonworks.com:2181");

        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        props.put(StreamsConfig.STATE_DIR_CONFIG,"/tmp");

        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);

        //building Kafka Streams Model
        KStreamBuilder kStreamBuilder = new KStreamBuilder();
        //the source of the streaming analysis is the topic with country messages
        KStream<String, String> stream = kStreamBuilder.stream(Serdes.String(), Serdes.String(), "event_attendees_raw");
        //transform
        KStream<String, String> result = stream.flatMap((k, v) -> transform(k, v)).filter((k, v) -> v != null && v.length() > 0);
        //publish
        result.to(Serdes.String(), Serdes.String(), "event_attendees");

        //create an instance of StreamsConfig from the Properties instance
        StreamsConfig config = new StreamsConfig(props);

        (new KafkaStreams(kStreamBuilder, config)).start();

    }


    private static Iterable<KeyValue<String, String>> transform(String key, String value) {
        //create
        List<KeyValue<String, String>> items = new ArrayList<KeyValue<String, String>>();
        //split
        String[] fields = value.split(",",  -1);
        //check
        if ( isHeader(fields) || !isValid(fields) ) {
            //add
            items.add(new KeyValue<>(key, ""));
        }
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

        List<String[]> results = new ArrayList<String[]>();

        String event_id = fields[0];

        if ( fields.length > 1 && fields[1] != null ) {
            //split
            String[] yesUsers = fields[1].split(" ");
            //check
            if ( yesUsers != null && yesUsers.length > 0 ) {
                //loop
                for ( String yesUser : yesUsers ) {
                    //add
                    results.add(new String[] { event_id, yesUser, "yes" });
                }
            }
        }

        if ( fields.length > 2 && fields[2] != null ) {
            //split
            String[] maybeUsers = fields[2].split( " " );
            //check
            if ( maybeUsers != null && maybeUsers.length > 0 ) {
                //loop
                for ( String maybeUser : maybeUsers ) {
                    //add
                    results.add(new String[] { event_id, maybeUser, "maybe" });
                }
            }
        }

        if ( fields.length > 3 && fields[3] != null ) {
            //split
            String[] invitedUsers = fields[3].split( " " );
            //check
            if ( invitedUsers != null && invitedUsers.length > 0 ) {
                //loop
                for ( String invitedUser : invitedUsers ) {
                    //add
                    results.add(new String[] { event_id, invitedUser, "invited" });
                }
            }
        }

        //status - no
        if ( fields.length > 4 && fields[4] != null ) {
            //split
            String[] noUsers = fields[4].split( " " );
            //check
            if ( noUsers != null && noUsers.length > 0 ) {
                //loop
                for ( String noUser : noUsers ) {
                    //add
                    results.add(new String[] { event_id, noUser, "no" });
                }
            }
        }
        return results;
    }

    protected static Boolean isHeader(String[] fields) {
        //check
        return (isValid(fields) && fields[0].equals("event") && fields[1].equals("yes") && fields[2].equals("maybe") && fields[3].equals("invited") && fields[4].equals("no"));
    }


    protected static Boolean isValid(String[] fields) {
        //check - evnet_id, yes, maybe, invited, no
        return (fields.length > 4);
    }
}
