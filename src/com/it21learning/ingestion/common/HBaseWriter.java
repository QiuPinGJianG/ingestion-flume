package com.it21learning.ingestion.common;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import com.it21learning.ingestion.config.IT21Config;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

public class HBaseWriter implements Persistable {
	//core-site xml file
	private String coreSite = null;
	//hdfs-site xml file
	private String hdfsSite = null;
	//hbase-site xml file
	private String hbaseSite = null;
	
	//the hbase table
	private String hbTable = null;
	
	//the parser
	private Parsable<Put> parser = null;
	
	//constructor
	public HBaseWriter(String hbTable, Parsable<Put> parser) {
		//set
		this.hbTable = hbTable;
		this.parser = parser;
	}

	//initialize to extract the hbase-site configuration
	public void initialize(Properties props) {
		//core-site
		this.coreSite = props.getProperty(IT21Config.coreSite);
		//hdfs-site
		this.hdfsSite = props.getProperty(IT21Config.hdfsSite);
		//hbase
		this.hbaseSite = props.getProperty(IT21Config.hbaseSite);
	}
	
	//write
	public int write( ConsumerRecords<String, String> records ) throws Exception {
		//the # of records puts
		int numPuts = 0;
		//check
		if ( this.hbaseSite == null || this.hbaseSite.isEmpty() ) {
			//error out
			throw new Exception("The hbase-site.xml is not initialized.");
		}
		//configuration
		Configuration cfg = HBaseConfiguration.create();
		//check
		if ( this.coreSite != null ) {
			//set resource
			cfg.addResource( new Path(this.coreSite) );
		}
		if ( this.hdfsSite != null ) {
			//set resource
			cfg.addResource( new Path(this.hdfsSite) );
		}
		//the hbase-site
		cfg.addResource( new Path(this.hbaseSite) );
		
		//establish a connection
		Connection conn = ConnectionFactory.createConnection( cfg );
		try {
			//HTable
			Table tbl = conn.getTable( TableName.valueOf(this.hbTable) );
			try {
				//collection
				List<Put> puts = new ArrayList<Put>();
				//flags
				long passHead = 0;
				//loop    
				for ( ConsumerRecord<String, String> record : records ) {
					//parse event record
					String[] elements = record.value().split(",", -1 );
					//check if the head has been passed
					if ( passHead == 0 && this.parser.isHeader(elements) ) {
						//flag
						passHead = 1;
						//skip
						continue;
					}	
	
					//parse
					if ( this.parser.isValid(elements) ) {
						//add 
						puts.add( this.parser.parse(elements) );
					}
				}
				//check
				if ( puts.size() > 0 ) {
					//save
					tbl.put( puts );
				}
				
				//set
				numPuts = puts.size();
			}
			finally {
				//close the table
				tbl.close();
			}
		}
		finally {
			//close the connection
		    conn.close();
	    }
		return numPuts;
	}
}
