package com.it21learning.ingestion.kafka;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.it21learning.ingestion.common.HBaseWriter;
import com.it21learning.ingestion.common.Parsable;
import com.it21learning.ingestion.common.Persistable;
import com.it21learning.ingestion.kafka.UserConsumer.UserHBaseParser;

public class UserFriendsConsumer extends IT21Consumer {
	//hbase parser
	public static class UserFriendsHBaseParser implements Parsable<Put> {
		//check if the record is a header
		public Boolean isHeader(String[] fields) {
			//check
	        return (isValid(fields) && fields[0].equals("user_id") && fields[1].equals("friend_id"));
		}
		
		//check if a record is valid
		public Boolean isValid(String[] fields) {
			//check
	        return (fields.length > 1);
		}
		
		//parse the record
		public Put parse(String[] fields) {
			//user id
			Put p = new Put(Bytes.toBytes( (fields[0] + "." + fields[1]).hashCode() ));

			//profile: uf
			p = p.addColumn(Bytes.toBytes("uf"), Bytes.toBytes("user_id"), Bytes.toBytes(fields[0]));
			//profile: uf
			p = p.addColumn(Bytes.toBytes("uf"), Bytes.toBytes("friend_id"), Bytes.toBytes(fields[1]));
			   
			//result
		    return p;
        }
	}
	
	//kafka topic
	@Override
	protected String getKafkaTopic() {
		return "user_friends";
	}
	//the flag for how to commit the consumer reads
	@Override
	protected Boolean getKafkaAutoCommit() {
		return false;
	}
	//the max # of records polled
	@Override
	protected int getMaxPolledRecords() {
		return 32000;
	}
	//consumer group
	@Override
	protected String getKafkaConsumerGrp() {
		return "grpUserFriends";
	}

	//constructor
	public UserFriendsConsumer() {
		//call base
		super(new Persistable[] { new HBaseWriter("events_db:user_friend", new UserFriendsHBaseParser()) });
	}
}
