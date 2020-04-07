package com.it21learning.ingestion.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.List;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.fs.Path;

import com.it21learning.ingestion.IngestionExecutor;
import com.it21learning.ingestion.common.Persistable;
import com.it21learning.ingestion.config.IT21Config;

public abstract class IT21Consumer implements IngestionExecutor {
	//property - kafka broker url
	private String kafkaBrokerUrl = null;
	//kafka topic
	protected abstract String getKafkaTopic();
	//the flag for how to commit the consumer reads
	protected abstract Boolean getKafkaAutoCommit();
	//the max # of records polled
	protected int getMaxPolledRecords() {
		return 6400;
	}
	//consumer group
	protected abstract String getKafkaConsumerGrp();
	
	//writers
	private Persistable[] writers = null;
	
	//constructor
	public IT21Consumer(Persistable[] writers) {
		//set
		this.writers = writers;
	}
	
	//initialize the properties
	public void initialize(Properties props) {
		//load
		this.kafkaBrokerUrl = props.getProperty(IT21Config.kafkaBrokerUrl);
		//check
		if ( this.writers != null && this.writers.length > 0 ) {
			//initialize
			for ( Persistable writer: writers ) {
				//call
				writer.initialize(props);
			}
		}
	}

	//consume
	protected void consume() throws Exception {
		//check
		if ( this.kafkaBrokerUrl == null || this.kafkaBrokerUrl.isEmpty() ) {
			//error out
			throw new Exception("The Kafka broker url is not initialized.");
		}
		//print
		System.out.println("The Kafka BrokerUrl --> " + this.kafkaBrokerUrl);
		//prepare properties for the consumer
		Properties props = new Properties();
		props.put("bootstrap.servers", this.kafkaBrokerUrl);
		props.put("group.id", this.getKafkaConsumerGrp());
		props.put("enable.auto.commit", this.getKafkaAutoCommit() ? "true" : "false");
		props.put("auto.offset.reset", "earliest");
		props.put("request.timeout.ms", "180000");
		props.put("session.timeout.ms", "120000");
		props.put("max.poll.records", Integer.toString(this.getMaxPolledRecords()));
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		//create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		//the topics
		List<TopicPartition> topics = Arrays.asList(new TopicPartition(getKafkaTopic(), 0));
		//assign
		consumer.assign(topics);
		//start from the beginning
		consumer.seek(topics.get(0), 0L);
		//print message
		System.out.println("Consumer subscribed to topic -> " + getKafkaTopic());
		
		try {
			//loop for reading   
			while ( true ) {
			    //poll records
				ConsumerRecords<String, String> records = consumer.poll( 1000 );
				//number of records
				int recordsCount = (records != null) ? records.count() : 0;
				//check
				if ( recordsCount <= 0 ) {
					//sleep for 5 seconds
					Thread.sleep(3000);
					//go next
					continue;
				}
				//print
				System.out.print(String.format("%d messages polled ...", recordsCount));
				
				//check
				if ( this.writers != null && this.writers.length > 0 && recordsCount > 0 ) {
					//writers
					for (Persistable writer: writers) {
						//write
						writer.write( records );
					}
					
					//check
					if ( !this.getKafkaAutoCommit() ) {
						//commit
						consumer.commitSync();
					}
			    }
				//print
				System.out.println("  processed.");
			}  
		}
		finally {
			//close
			consumer.close();
		}
	}

	//main entry
	public void execute(String[] args) throws Exception {
		//check
		if (args.length < 1) {
			System.out.println(String.format("Usage: %s <settings-file>", this.getClass().getName()));
			System.out.println("<settings-file>: the configuration settings");
			System.out.println("<target>: the target place for persisting event data. It must be either cassandra or hbase");
		}
		else {
			//initialize
			this.initialize(IT21Config.loadSettings(args[0]));
			//consume & persist to hbase
			this.consume();
		}
	}
}
