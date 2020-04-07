package com.it21learning.ingestion.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import com.it21learning.ingestion.IngestionExecutor;
import com.it21learning.ingestion.config.IT21Config;

public abstract class IT21Streamer implements IngestionExecutor {
	//property - kafka zookeeper url
	private String zooKeeperUrl = null;
	//property - kafka broker url
	private String kafkaBrokerUrl = null;
	//property - state directory
	private String stateDir = null;
	
	//application id
	protected abstract String getApplicationId();
	//the source topic
	protected abstract String getSourceTopic();
	//the target topic
	protected abstract String getTargetTopic();

	//constructor
	public IT21Streamer() {
	}
	
	//initialize the properties
	public void initialize(Properties props) {
		//load
		this.zooKeeperUrl = props.getProperty(IT21Config.zooKeeperUrl);
		this.kafkaBrokerUrl = props.getProperty(IT21Config.kafkaBrokerUrl);
		this.stateDir = props.getProperty(IT21Config.stateDir);
	}
	
	//execute
	protected void stream() throws Exception {
		 Properties properties = new Properties();
		 //application id
		 properties.put(StreamsConfig.APPLICATION_ID_CONFIG, getApplicationId());
	     //Kafka bootstrap server 
		 properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerUrl);
         //Apache ZooKeeper  
		 properties.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zooKeeperUrl);
         //default serdes for serialzing and deserializing key and value from and to streams 
		 properties.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		 properties.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
         //the state directory
		 properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
	     //work around for ProducerRecords produced by Kafka 0.9.0 or earlier - see: https://groups.google.com/forum/#!topic/confluent-platform/5oT0GRztPBo
		 // 解BUG
		 properties.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
		 
	     //building Kafka Streams Model
         KStreamBuilder kStreamBuilder = new KStreamBuilder();
         //the source of the streaming analysis is the topic with country messages
	     KStream<String, String> stream = kStreamBuilder.stream(Serdes.String(), Serdes.String(), this.getSourceTopic());
	     //transform -> 全局transfrom
	     KStream<String, String> result = stream.flatMap((k, v) -> transform(k, v)).filter((k, v) -> v != null && v.length() > 0);
	     //publish
	     result.to(Serdes.String(), Serdes.String(), this.getTargetTopic());
	     
		 //create an instance of StreamsConfig from the Properties instance
	     StreamsConfig config = new StreamsConfig( properties );
	     //start
	     (new KafkaStreams(kStreamBuilder, config)).start();
	}

	//transform the record data
	// 全局transfrom
	private Iterable<KeyValue<String, String>> transform(String key, String value) {
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
			for (String[] vs: transform(fields)) { // fields -> user_id , friend_id
				                                   // event_id , user_id , type(maybe, yes , no, invited)
				//add
				items.add(new KeyValue<>(key, String.join(",", vs)));
			}
		}
		return items;
	}
	// 1, KeyValue 是什么?
	// 2, transform(fields)是集合，为什么for中String[] vs?
	// 3，局部的user_id 一定会多， 那么source topic input 的key  为什么也会增加?
    // 4, default serdes for serialzing and deserializing key and value from and to streams
	//		 properties.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
	//		 properties.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName()); ?
	// 5,applicationId 是什么？
	//transform the record data
	protected abstract List<String[]> transform(String[] fields);
	//check if the record is a header
	protected abstract Boolean isHeader(String[] fields);
	//check if a record is valid
	protected abstract Boolean isValid(String[] fields);
	
	//main entry
	//protected static <T extends IT21Consumer> void start(Class<T> clsConsumer, String[] args) throws Exception {
	public void execute(String[] args) throws Exception {
		//check
		if (args.length < 1) {
			System.out.println(String.format("Usage: %s <settings-file>", this.getClass().getName()));  //
			System.out.println("<settings-file>: the configuration settings");
		}
		else {
			try {
				//initialize
				this.initialize(IT21Config.loadSettings(args[0]));
				//stream
				this.stream();
			}
			catch ( Exception e ) {
				//print error
				e.printStackTrace();
			}
		}
	}
}
