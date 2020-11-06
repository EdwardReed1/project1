#Project 1 Hive Queries

#Question 1

CREATE DATABASE PROJECT_DB;

CREATE TABLE PAGEVIEWS(CODE STRING,  TITLE STRING, VIEWS BIGINT, RESPONSE BIGINT) 
ROW FORMAT DELIMETED 
FIELDS TERMINATED BY ‘ ‘;

#the dataset it retrieved using the linux command wget https://dumps.wikimedia.org/other/pageviews/2020/2020-10/pageviews-20201020-{00..23}0000.gz

#the dataset is all put into the same folder and the path in the statement is the path to that folder

LOAD DATA LOCAL INPATH '/home/rej99/pageviews' INTO TABLE PAGEVIEWS

SELECT TITLE, SUM(VIEWS) AS TOTAL_VIEWS FROM PAGEVIEWS
WHERE CODE='en' OR CODE='en.m'
GROUP BY TITLE
ORDER BY TOTAL_VIEWS DESC;

#Question 2

#retrieve the dataset for clickstream with the linux command wget https://meta.wikimedia.org/wiki/Research:Wikipedia_clickstream
#create new table for clickstream dataset

CREATE TABLE CLICKSTREAM(PREV STRING, CURR STRING, TYPE STRING, CLICKS BIGINT) 
ROW FORMAT DELIMETED 
FIELDS TERMINATED BY ‘\t‘;

LOAD DATA LOCAL INPATH '/home/rej99/clickstream-enwiki-2020-09.tsv' INTO TABLE CLICKSTREAM;

#create smaller sub-table using a query on the clickstream table

CREATE TABLE TOTAL_CLICKSTREAM ROW FORMAT DELIMITED FIELDS TERMINATED BY ‘\t’ AS
SELECT TITLE,  SUM(NUMBER) AS TOTAL_CLICKS FROM CLICKSTREAM
WHERE UPPER(TYPE)=‘internal’;

#turns the result of the previous question into its own table
CREATE TABLE TOTAL_VIEWS ROW FORMAT DELIMETED FIELDS TERMINATED BY ' ' AS
SELECT TITLE, SUM(VIEWS) AS TOTAL_VIEWS FROM PAGEVIEWS
WHERE CODE='en' OR CODE='en.m'
GROUP BY TITLE
ORDER BY TOTAL_VIEWS DESC;

#run inner join on the two sub tables
SELECT TOTAL_VIEWS.TITLE, TOTAL_VIEWS.VIEWS, TOTAL_CLICKSTREAM.TOTAL_CLICKS,  ROUND(TOTAL_CLICKSTREAM.TOTAL_CLICKS / TOTAL_VIEWS.VIEWS * 100, 2) AS PERCENTAGE_CLICKED FROM TOTAL_VIEWS
INNER JOIN TOTAL_CLICKSTREAM ON TOTAL_CLICKSTREAM.PREV = TOTAL_VIEWS.TITLE
WHERE TOTAL_VIEWS.VIEWS > 1000000;
ORDER BY PERCENTAGE_CLICKED
LIMIT 15; 

#Question 3
SELECT TOTAL_VIEWS.TITLE, TOTAL_VIEWS.VIEWS, TOTAL_CLICKSTREAM.TOTAL_CLICKS,  ROUND(TOTAL_CLICKSTREAM.TOTAL_CLICKS / TOTAL_VIEWS.VIEWS * 100, 2) AS PERCENTAGE_CLICKED FROM TOTAL_VIEWS
INNER JOIN TOTAL_CLICKSTREAM ON TOTAL_CLICKSTREAM.PREV = TOTAL_VIEWS.TITLE
WHERE TOTAL_VIEWS.TITLE LIKE 'Hotel California';
ORDER BY PERCENTAGE_CLICKED
LIMIT 15; 

SELECT TOTAL_VIEWS.TITLE, TOTAL_VIEWS.VIEWS, TOTAL_CLICKSTREAM.TOTAL_CLICKS,  ROUND(TOTAL_CLICKSTREAM.TOTAL_CLICKS / TOTAL_VIEWS.VIEWS * 100, 2) AS PERCENTAGE_CLICKED FROM TOTAL_VIEWS
INNER JOIN TOTAL_CLICKSTREAM ON TOTAL_CLICKSTREAM.PREV = TOTAL_VIEWS.TITLE
WHERE TOTAL_VIEWS.TITLE LIKE 'Hotel California (Eagles Album)';
ORDER BY PERCENTAGE_CLICKED
LIMIT 15; 

SELECT TOTAL_VIEWS.TITLE, TOTAL_VIEWS.VIEWS, TOTAL_CLICKSTREAM.TOTAL_CLICKS,  ROUND(TOTAL_CLICKSTREAM.TOTAL_CLICKS / TOTAL_VIEWS.VIEWS * 100, 2) AS PERCENTAGE_CLICKED FROM TOTAL_VIEWS
INNER JOIN TOTAL_CLICKSTREAM ON TOTAL_CLICKSTREAM.PREV = TOTAL_VIEWS.TITLE
WHERE TOTAL_VIEWS.TITLE LIKE 'The Long Run';
ORDER BY PERCENTAGE_CLICKED
LIMIT 15; 

SELECT TOTAL_VIEWS.TITLE, TOTAL_VIEWS.VIEWS, TOTAL_CLICKSTREAM.TOTAL_CLICKS,  ROUND(TOTAL_CLICKSTREAM.TOTAL_CLICKS / TOTAL_VIEWS.VIEWS * 100, 2) AS PERCENTAGE_CLICKED FROM TOTAL_VIEWS
INNER JOIN TOTAL_CLICKSTREAM ON TOTAL_CLICKSTREAM.PREV = TOTAL_VIEWS.TITLE
WHERE TOTAL_VIEWS.TITLE LIKE 'Eagles Live';
ORDER BY PERCENTAGE_CLICKED
LIMIT 15; 

#Question 4

#divide the pageviews files into three sections with Austrailia having the files from 04-10, the US having 12-18, and the UK having 18-00
#put those files into appropriate folders.

#create UK and filter table

CREATE TABLE UK_VIEWS(CODE STRING,  TITLE STRING, VIEWS BIGINT, RESPONSE BIGINT) 
ROW FORMAT DELIMETED 
FIELDS TERMINATED BY ‘ ‘;

LOAD DATA LOCAL INPATH 'home/rej99/UK_Views' INTO TABLE UK_VIEWS

SELECT TITLE, SUM(VIEWS) AS TOTAL_UK_VIEWS FROM UK_VIEWS
GROUP BY TITLE
ORDER BY TOTAL_UK_VIEWS
LIMIT 50;


#create US and filter table

CREATE TABLE US_VIEWS(CODE STRING,  TITLE STRING, VIEWS BIGINT, RESPONSE BIGINT) 
ROW FORMAT DELIMETED 
FIELDS TERMINATED BY ‘ ‘;

LOAD DATA LOCAL INPATH 'home/rej99/US_Views' INTO TABLE US_VIEWS

SELECT TITLE, SUM(VIEWS) AS TOTAL_US_VIEWS FROM US_VIEWS
GROUP BY TITLE
ORDER BY TOTAL_US_VIEWS
LIMIT 50;

#create AUS and filter table

CREATE TABLE AUS_VIEWS(CODE STRING,  TITLE STRING, VIEWS BIGINT, RESPONSE BIGINT) 
ROW FORMAT DELIMETED 
FIELDS TERMINATED BY ‘ ‘;

LOAD DATA LOCAL INPATH 'home/rej99/AUS_Views' INTO TABLE AUS_VIEWS

SELECT TITLE, SUM(VIEWS) AS TOTAL_AUS_VIEWS FROM AUS_VIEWS
GROUP BY TITLE
ORDER BY TOTAL_AUS_VIEWS
LIMIT 50;


#Question 5

#retrieve the dataset for wikimedia events with wget https://dumps.wikimedia.org/other/mediawiki_history/2020-09/enwiki/2020-09.enwiki.2020-09.tsv.bz2

#put the 70 column table into a database with this create query

create table revisions (WIKI_DB STRING, 
                EVENT_ENTITY STRING,
                EVENT_TYPE STRING,
                EVENT_TIMESTAMP STRING,
                EVENT_COMMENT STRING,
                EVENT_USER_ID BIGINT,
                EVENT_USER_TEXT_HISTORICAL STRING,
                EVENT_USER_TEXT STRING,
                EVENT_USER_BLOCKS_HISTORICAL STRING,
                EVENT_USER_BLOCKS ARRAY<STRING>,
                EVENT_USER_GROUPS_HISTORICAL ARRAY<STRING>,
                EVENT_USER_GROUPS ARRAY<STRING>,
                event_user_is_bot_by_historical ARRAY<STRING>,
                event_user_is_bot_by ARRAY<STRING>,
                event_user_is_created_by_self BOOLEAN,
                event_user_is_created_by_system BOOLEAN,
                event_user_is_created_by_peer BOOLEAN,
                event_user_is_anonymous BOOLEAN,
                event_user_registration_timestamp STRING,
                event_user_creation_timestamp STRING,
                event_user_first_edit_timestamp STRING,
                event_user_revision_count BIGINT,
                event_user_seconds_since_previous_revision BIGINT,
                page_id BIGINT,
                page_title_historical STRING,
                page_title STRING,
                page_namespace_historical INT,
                page_namespace_is_content_historical BOOLEAN,
                page_namespace INT,
                page_namespace_is_content BOOLEAN,
                page_is_redirect BOOLEAN,
                page_is_deleted BOOLEAN,
                page_creation_timestamp STRING,
                page_first_edit_timestamp STRING,
                page_revision_count BIGINT,
                page_seconds_since_previous_revision BIGINT,
                user_id BIGINT,
                user_text_historical STRING,
                user_text STRING,
                user_blocks_historical ARRAY<STRING>,
                user_blocks ARRAY<STRING>,
                user_groups_historical ARRAY<STRING>,
                user_groups ARRAY<String>,
                user_is_bot_by_historical ARRAY<STRING>,
                user_is_bot_by Array<STRING>,
                user_is_created_by_self BOOLEAN,
                user_is_created_by_system boolean,
                user_is_created_by_peer BOOLEAN,
                user_is_anonymous boolean,
                user_registration_timestamp String,
                user_creation_timestamp STRING,
                user_first_edit_timestamp STRING,
                revision_id bigint,
                revision_parent_id bigint,
                revision_minor_edit boolean,
                revision_deleted_parts Array<String>,
                revision_deleted_parts_are_suppressed boolean,
                revision_text_bytes bigint,
                revision_text_bytes_diff bigint,
                revision_text_sha1 string,
                revision_content_model string,
                revision_content_format string,
                revision_is_deleted_by_page_deletion boolean,
                revision_deleted_by_page_deletion_timestamp string,
                revision_is_identity_reverted boolean,
                revision_first_identity_reverting_revision_id bigint,
                revision_seconds_to_identity_revert bigint,
                revision_is_identity_revert boolean,
                revision_is_from_before_page_creation boolean,
                revision_tags Array<string>
                )
            ROW FORMAT DELIMITED 
            FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH '/home/rej99/2020-09.enwiki.2020-09.tsv' INTO TABLE REVISIONS;

CREATE TABLE FILTERED_REVISIONS ROW FORMAT DELIMETED FIELDS TERMINATED BY '\t' AS
SELECT PAGE_TITLE, REVISION_SECONDS_TO_IDENTITY_REVERT AS SECONDS FROM REVISIONS
WHERE REVISION_SECONDS_TO_IDENTITY_REVERT > 0;

CREATE TABLE VIEWS_WITH_REVISIONS ROW FORMAT DELIMETED FIELDS TERMINATED BY '\t' AS
SELECT TOTAL_VIEWS.TITLE, FILTERED_REVISIONS.SECONDS, TOTAL_VIEWS.VIEWS FROM TOTAL_VIEWS
INNER JOIN FILTERED_REVISIONS ON FILTERED_REVISIONS.TITLE = TOTAL_VIEWS.TITLE;

SELECT ROUND(AVG(TOTAL_VIEWS.VIEWS * FILTERED_REVISIONS.SECONDS / 86400), 2) AS VIEWS_PER_PAGE FROM TOTAL_VIEWS;



#Question 6
 
SELECT EVENT_TYPE, COUNT(EVENT_TYPE) AS TOTAL_PER_TYPE FROM REVISIONS
ORDER BY TOTAL_PER_TYPE;

