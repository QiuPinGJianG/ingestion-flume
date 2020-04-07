package com.it21learning.ingestion.kafka;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.it21learning.ingestion.common.HBaseWriter;
import com.it21learning.ingestion.common.Parsable;
import com.it21learning.ingestion.common.Persistable;

public class EventConsumer extends IT21Consumer {
	//hbase parser
	public static class EventHBaseParser implements Parsable<Put> {
		//check if the record is a header
		public Boolean isHeader(String[] fields) {
			//check
	        return (isValid(fields) && fields[0].equals("event_id") && fields[1].equals("user_id") && fields[2].equals("start_time")
	                && fields[3].equals("city") && fields[4].equals("state") && fields[5].equals("zip") && fields[6].equals("country")
	                && fields[7].equals("lat") && fields[8].equals("lng"));
		}
		
		//check if a record is valid
		public Boolean isValid(String[] fields) {
			//check
	        return (fields.length > 8);
		}
		
		//parse the record
		public Put parse(String[] fields) {
	        //event id
	        Put p = new Put(Bytes.toBytes(fields[0]));

	        //schedule: start_time
	        p = p.addColumn(Bytes.toBytes("schedule"), Bytes.toBytes("start_time"), Bytes.toBytes(fields[2]));

	        //location: city
	        p = p.addColumn(Bytes.toBytes("location"), Bytes.toBytes("city"), Bytes.toBytes(fields[3]));
	        //location: state
	        p = p.addColumn(Bytes.toBytes("location"), Bytes.toBytes("state"), Bytes.toBytes(fields[4]));
	        //location: zip
	        p = p.addColumn(Bytes.toBytes("location"), Bytes.toBytes("zip"), Bytes.toBytes(fields[5]));
	        //location: country
	        p = p.addColumn(Bytes.toBytes("location"), Bytes.toBytes("country"), Bytes.toBytes(fields[6]));
	        //location: lat
	        p = p.addColumn(Bytes.toBytes("location"), Bytes.toBytes("lat"), Bytes.toBytes(fields[7]));
	        //location: lng
	        p = p.addColumn(Bytes.toBytes("location"), Bytes.toBytes("lng"), Bytes.toBytes(fields[8]));
	        //user id
	        p = p.addColumn(Bytes.toBytes("creator"), Bytes.toBytes("user_id"), Bytes.toBytes(fields[1]));

	        //remark
			StringBuffer sb = new StringBuffer();
			//check
			if ( fields.length > 8 ) {
				//check
				for ( int i = 9; i < fields.length; i++ ) {
					//check
					if ( sb.length() > 0 ) 
						sb.append( "|" );
					//append
					sb.append( fields[i] );
				}
			}
	        //remark
			p = p.addColumn(Bytes.toBytes("remark"), Bytes.toBytes("common_words"), Bytes.toBytes(sb.toString()));

	        //creator: user_id
	        return p;
        }
	}
	
	//kafka topic
	@Override
	protected String getKafkaTopic() {
		return "events";
	}
	//the flag for how to commit the consumer reads
	@Override
	protected Boolean getKafkaAutoCommit() {
		return true;
	}
	//the max # of records polled
	@Override
	protected int getMaxPolledRecords() {
		return 32000;
	}
	//consumer group
	@Override
	protected String getKafkaConsumerGrp() {
		return "grpEvents";
	}
	
	//constructor
	public EventConsumer() {
		//call base
		super(new Persistable[] { new HBaseWriter("events_db:events", new EventHBaseParser()) });
	}
}
