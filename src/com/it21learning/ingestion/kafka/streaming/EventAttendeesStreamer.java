package com.it21learning.ingestion.kafka.streaming;

import java.util.ArrayList;
import java.util.List;

import com.it21learning.ingestion.kafka.IT21Streamer;

public class EventAttendeesStreamer extends IT21Streamer {
	//application id
	@Override
	protected String getApplicationId() {
		return "it21learning-event-attendees-streamming";
	};
	//the source topic
	@Override
	protected String getSourceTopic() {
		return "event_attendees_raw";
	}
	//the target topic
	@Override
	protected String getTargetTopic() {
		return "event_attendees";
	}

	//transform the record data
	@Override
	protected List<String[]> transform(String[] fields) {
		//put collection
		List<String[]> results = new ArrayList<String[]>();
        //event
        String event_id = fields[0];

        //status - yes
        if ( fields.length > 1 && fields[1] != null ) {
        	//split
        	String[] yesUsers = fields[1].split(" ");
        	//check
        	if ( yesUsers != null && yesUsers.length > 0 ) {
        		//loop
        		for ( String yesUser : yesUsers ) {
        			//add
        			results.add(new String[] { event_id, yesUser, "yes" });
        		}             
        	}
        }   
  
        //status - maybe
        if ( fields.length > 2 && fields[2] != null ) {
        	//split
        	String[] maybeUsers = fields[2].split( " " );
        	//check
        	if ( maybeUsers != null && maybeUsers.length > 0 ) {
        		//loop
        		for ( String maybeUser : maybeUsers ) {
        			//add
        			results.add(new String[] { event_id, maybeUser, "maybe" });
        		}             
        	}
        }   

        //status - invited
        if ( fields.length > 3 && fields[3] != null ) {
        	//split
        	String[] invitedUsers = fields[3].split( " " );
        	//check
        	if ( invitedUsers != null && invitedUsers.length > 0 ) {
        		//loop
        		for ( String invitedUser : invitedUsers ) {
        			//add
        			results.add(new String[] { event_id, invitedUser, "invited" });
        		}             
        	}
        }   

        //status - no
        if ( fields.length > 4 && fields[4] != null ) {
        	//split
        	String[] noUsers = fields[4].split( " " );
        	//check
        	if ( noUsers != null && noUsers.length > 0 ) {
        		//loop
        		for ( String noUser : noUsers ) {
        			//add
        			results.add(new String[] { event_id, noUser, "no" });
        		}             
        	}
        }  
        return results;
    }
	
	//check if the record is a header
	@Override
	protected Boolean isHeader(String[] fields) {
		//check
        return (isValid(fields) && fields[0].equals("event") && fields[1].equals("yes") && fields[2].equals("maybe") && fields[3].equals("invited") && fields[4].equals("no"));
	}
	
	//check if a record is valid
	@Override
	protected Boolean isValid(String[] fields) {
		//check - evnet_id, yes, maybe, invited, no
        return (fields.length > 4);
	}
}
