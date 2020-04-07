package com.it21learning.ingestion.common;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface Persistable {
	//initialize
	void initialize(Properties props);
	
	//write
	int write( ConsumerRecords<String, String> records ) throws Exception;
}
