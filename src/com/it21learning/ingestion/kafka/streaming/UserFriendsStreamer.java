package com.it21learning.ingestion.kafka.streaming;

import java.util.ArrayList;
import java.util.List;

import com.it21learning.ingestion.kafka.IT21Streamer;

public class UserFriendsStreamer extends IT21Streamer {
	//application id
	@Override
	protected String getApplicationId() {
		return "it21learning-user-friends-streamming";
	};
	//the source topic
	@Override
	protected String getSourceTopic() {
		return "user_friends_raw";
	}
	//the target topic
	@Override
	protected String getTargetTopic() {
		return "user_friends";
	}

	//transform the record data

	// friends -> explode -> friend
	@Override
	protected List<String[]> transform(String[] fields) { // fields -> user_id , friend_id
		//put collection
		List<String[]> results = new ArrayList<String[]>();
        //user
        String user = fields[0];
        //friends
        String[] friends = fields[1].split(" ");
        //check
        if ( friends != null && friends.length > 0 ) {
        	//loop
        	for ( String friend : friends ) {
    			//add
    			results.add(new String[] { user, friend });
           	}
        }
        return results;
    }
	
	//check if the record is a header
	@Override
	protected Boolean isHeader(String[] fields) {
		//check
        return (isValid(fields) && fields[0].equals("user") && fields[1].equals("friends"));
	}
	
	//check if the record is a header
	@Override
	protected Boolean isValid(String[] fields) {
		//check
        return (fields.length > 1);
	}
}
