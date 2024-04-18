-- projects/2a/select_out.hql
INSERT OVERWRITE DIRECTORY 'noone1234567_hiveout'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES terminated by '\n'
stored as textfile
SELECT * FROM hw2_pred;

