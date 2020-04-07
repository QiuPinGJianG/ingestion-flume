-- create database if it doesn't exists
CREATE DATABASE IF NOT EXISTS events;
-- make events as the current database
use events;

-- CREATE TABLE predictions
DROP TABLE IF EXISTS predictions; 
-- create
CREATE TABLE IF NOT EXISTS predictions(                                                                         
    prediction_id int AUTO_INCREMENT,
    user_id varchar(32),
    event_id varchar(32),
    user_interested int,
    prediction_time varchar(32),
    PRIMARY KEY (prediction_id)
);

-- CREATE TABLE event_attendee
DROP TABLE IF EXISTS event_attendee;
-- create
CREATE TABLE IF NOT EXISTS event_attendee(
	event_attend_id int AUTO_INCREMENT,
	event_id varchar(32),
	user_id varchar(32),
	attend_type varchar(16),
	PRIMARY KEY(event_attend_id)
);

-- CREATE TABLE event_attendee_count
DROP TABLE IF EXISTS event_attendee_count;
-- create
CREATE TABLE IF NOT EXISTS event_attendee_count(
	event_attend_id int AUTO_INCREMENT,
	event_id varchar(32),
	attend_type varchar(16),
	attend_count int,
	PRIMARY KEY(event_attend_id)
);

-- CREATE TABLE event_cities
DROP TABLE IF EXISTS event_cities;
-- create 
CREATE TABLE IF NOT EXISTS event_cities(
	city_id int AUTO_INCREMENT,
	city varchar(64),
	level int,
	PRIMARY KEY(city_id)
);

-- CREATE TABLE event_countries
DROP TABLE IF EXISTS event_countries;
-- create 
CREATE TABLE IF NOT EXISTS event_countries(
	country_id int AUTO_INCREMENT,
	country varchar(64),
	level int,
	PRIMARY KEY(country_id)
);

-- CREATE TABLE events
DROP TABLE IF EXISTS events;
-- create 
CREATE TABLE IF NOT EXISTS events(
	event_seq_no int AUTO_INCREMENT,
	event_id varchar(32),
	start_time varchar(32),
	city varchar(64),             	                    
	state varchar(64),
	zip varchar(16),
	country varchar(64),              	                    
	latitude float,
	longitude float,
	user_id varchar(64),
	PRIMARY KEY(event_seq_no)
);

-- CREATE TABLE friend_attend_summary
DROP TABLE IF EXISTS friend_attend_summary;
-- create 
CREATE TABLE IF NOT EXISTS friend_attend_summary(
	friend_attend_id int AUTO_INCREMENT,
	user_id varchar(32),
	event_id varchar(32),
	invited_friends_count int,
	attended_friends_count int,
	not_attended_friends_count int,
	maybe_attended_friends_count int,
	PRIMARY KEY(friend_attend_id)
);

-- CREATE TABLE locale
DROP TABLE IF EXISTS locale;
-- create 
CREATE TABLE IF NOT EXISTS locale(
	locale_id int,
	locale varchar(16),
	PRIMARY KEY(locale_id)
);

-- CREATE TABLE train
DROP TABLE IF EXISTS train;
-- create 
CREATE TABLE IF NOT EXISTS train(
	train_id int AUTO_INCREMENT,
	user_id varchar(32),
	event_id varchar(32),
	invited int,
	time_stamp varchar(32),
	interested int,
	PRIMARY KEY(train_id)
);

-- CREATE TABLE test
DROP TABLE IF EXISTS test;
-- create 
CREATE TABLE IF NOT EXISTS test(
	test_id int AUTO_INCREMENT,
	user_id varchar(32),
	event_id varchar(32),
	invited int,
	time_stamp varchar(32),
	PRIMARY KEY(test_id)
);


-- CREATE TABLE user_attend_event_count
DROP TABLE IF EXISTS user_attend_event_count;
-- create 
CREATE TABLE IF NOT EXISTS user_attend_event_count(
	user_id varchar(32),
	invited_count int,
	attended_count int,
	not_attended_count int,
	maybe_attended_count int,
	PRIMARY KEY(user_id)
);

-- CREATE TABLE user_event_count
DROP TABLE IF EXISTS user_event_count;
-- create 
CREATE TABLE IF NOT EXISTS user_event_count(
	user_id varchar(32),
	event_count int,
	PRIMARY KEY(user_id)
);


-- CREATE TABLE user_friend
DROP TABLE IF EXISTS user_friend;
-- create 
CREATE TABLE IF NOT EXISTS user_friend(
	user_attend_id int AUTO_INCREMENT,
	user_id varchar(32),
	friend_id varchar(32),
	PRIMARY KEY(user_attend_id)
);


-- CREATE TABLE user_friend_count
DROP TABLE IF EXISTS user_friend_count;
-- create 
CREATE TABLE IF NOT EXISTS user_friend_count(
	user_id varchar(32),
	friend_count int,
	PRIMARY KEY(user_id)
);

-- CREATE TABLE users
DROP TABLE IF EXISTS users;
-- create 
CREATE TABLE IF NOT EXISTS users(
	user_id varchar(32),
	birth_year int,
	gender varchar(8),
	locale varchar(8),              	                    
	location varchar(128),
	time_zone varchar(8),
	joined_at varchar(16),
	PRIMARY KEY(user_id)
);
