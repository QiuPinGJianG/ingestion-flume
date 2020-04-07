package com.it21learning.ingestion.kafka;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.it21learning.ingestion.common.HBaseWriter;
import com.it21learning.ingestion.common.Parsable;
import com.it21learning.ingestion.common.Persistable;
import com.it21learning.ingestion.kafka.EventAttendeesConsumer.EventAttendeesHBaseParser;

public class TrainConsumer extends IT21Consumer {
	//hbase parser
	public static class TrainHBaseParser implements Parsable<Put> {
		//check if the record is a header
		public Boolean isHeader(String[] fields) {
			//check
	        return (isValid(fields) && fields[0].equals("user") && fields[1].equals("event") && fields[2].equals("invited")
	                && fields[3].equals("time_stamp") && fields[4].equals("interested") && fields[5].equals("not_interested"));
		}
		
		//check if a record is valid
		public Boolean isValid(String[] fields) {
			//check
	        return (fields.length > 5);
		}
		
		//parse the record
		public Put parse(String[] fields) {
	        //key
	        Put p = new Put(Bytes.toBytes((fields[0] + "." + fields[1]).hashCode()));

	        //eu: user
	        p = p.addColumn(Bytes.toBytes("eu"), Bytes.toBytes("user"), Bytes.toBytes(fields[0]));
	        //eu:event
	        p = p.addColumn(Bytes.toBytes("eu"), Bytes.toBytes("event"), Bytes.toBytes(fields[1]));

	        //eu: invited
	        p = p.addColumn(Bytes.toBytes("eu"), Bytes.toBytes("invited"), Bytes.toBytes(fields[2]));
	        //eu:time_stamp
	        p = p.addColumn(Bytes.toBytes("eu"), Bytes.toBytes("time_stamp"), Bytes.toBytes(fields[3]));
	        //eu: interested
	        p = p.addColumn(Bytes.toBytes("eu"), Bytes.toBytes("interested"), Bytes.toBytes(fields[4]));
	        //eu: not_interested
	        p = p.addColumn(Bytes.toBytes("eu"), Bytes.toBytes("not_interested"), Bytes.toBytes(fields[5]));

	        //result
	        return p;
        }
	}
	
	//kafka topic
	@Override
	protected String getKafkaTopic() {
		return "train";
	}
	//the flag for how to commit the consumer reads
	@Override
	protected Boolean getKafkaAutoCommit() {
		return false;
	}
	//consumer group
	@Override
	protected String getKafkaConsumerGrp() {
		return "grpTrain";
	}
	
	//constructor
	public TrainConsumer() {
		//call base
		super(new Persistable[] { new HBaseWriter("events_db:train", new TrainHBaseParser()) });
	}
}
