package com.it21learning.ingestion.common;

public interface Parsable<T> {
	//check if the record is a header
	Boolean isHeader(String[] fields);
	//check if a record is valid
	Boolean isValid(String[] fields);
	
	//parse the record
	T parse(String[] fields);
}
