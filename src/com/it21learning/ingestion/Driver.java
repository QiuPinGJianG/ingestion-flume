package com.it21learning.ingestion;

import java.util.Arrays;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CassandraAutoConfiguration;

@SpringBootApplication
public class Driver implements CommandLineRunner {
	//the main entry
	public static void main(String[] args) {
		//run
		SpringApplication.run(Driver.class, args);
	}

	//execute
	@Override
	public void run(String... args) throws Exception {
		//check
		if ( args == null || args.length < 1 ) {
			//error out
			throw new Exception("Please specify the executor class.");
		}
		//java -jar event-ingest-kafka-1.0.0.jar com.it21learning.ingestion.kafka.streaming.EventAttendeesStreamer settings.properties
		//java -jar event-ingest-kafka-1.0.0.jar com.it21learning.ingestion.kafka.UserConsumer settings.properties hbase
		//java -jar event-ingest-kafka-1.0.0.jar com.it21learning.ingestion.kafka.IT21Producer settings.properties test /root/events/data/test.csv
		//create instance
		Object o = Class.forName(args[0]).newInstance();
		//check
		if ( o instanceof IngestionExecutor ) {
			((IngestionExecutor)o).execute(Arrays.copyOfRange(args, 1, args.length));
		}
		else {
			//error out
			throw new Exception("The specified Executor is not an IngestionExecutor.");
		}
	}
}

