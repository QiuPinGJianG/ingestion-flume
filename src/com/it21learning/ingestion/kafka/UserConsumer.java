package com.it21learning.ingestion.kafka;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.it21learning.ingestion.common.HBaseWriter;
import com.it21learning.ingestion.common.Parsable;
import com.it21learning.ingestion.common.Persistable;
import com.it21learning.ingestion.kafka.TrainConsumer.TrainHBaseParser;

public class UserConsumer extends IT21Consumer {
	//hbase parser
	public static class UserHBaseParser implements Parsable<Put> {
		//check if the record is a header
		public Boolean isHeader(String[] fields) {
			//check
	        return (isValid(fields) && fields[0].equals("user_id") && fields[1].equals("locale") && fields[2].equals("birthyear")
	                && fields[3].equals("gender") && fields[4].equals("joinedAt") && fields[5].equals("location") && fields[6].equals("timezone")); 
		}
		
		//check if a record is valid
		public Boolean isValid(String[] fields) {
			//check
	        return (fields.length > 6);
		}
		
		//parse the record
		public Put parse(String[] fields) {
			//user_id, locale, birth_year, gender, joined_at, location, time_zone
	        //user id
	        Put p = new Put(Bytes.toBytes(fields[0]));

	        //profile: birth_year
	        p = p.addColumn(Bytes.toBytes("profile"), Bytes.toBytes("birth_year"), Bytes.toBytes(fields[2]));
	        //profile: gender
	        p = p.addColumn(Bytes.toBytes("profile"), Bytes.toBytes("gender"), Bytes.toBytes(fields[3]));

	        //region: locale
	        p = p.addColumn(Bytes.toBytes("region"), Bytes.toBytes("locale"), Bytes.toBytes(fields[1]));
	        //region: location
	        p = p.addColumn(Bytes.toBytes("region"), Bytes.toBytes("location"), Bytes.toBytes(fields[5]));
	        //region: time-zone
	        p = p.addColumn(Bytes.toBytes("region"), Bytes.toBytes("time_zone"), Bytes.toBytes(fields[6]));
	        
	        //registration: joined_at
	        p = p.addColumn(Bytes.toBytes("registration"), Bytes.toBytes("joined_at"), Bytes.toBytes(fields[4]));

	        //result
	        return p; 
        }
	}
	
	//kafka topic
	@Override
	protected String getKafkaTopic() {
		return "users";
	}
	//the flag for how to commit the consumer reads
	@Override
	protected Boolean getKafkaAutoCommit() {
		return false;
	}
	//consumer group
	@Override
	protected String getKafkaConsumerGrp() {
		return "grpUsers";
	}
	
	//constructor
	public UserConsumer() {
		//call base
		super(new Persistable[] { new HBaseWriter("events_db:users", new UserHBaseParser()) });
	}
}
