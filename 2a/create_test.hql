-- projects/2a/create_test.hql
CREATE temporary EXTERNAL TABLE IF NOT EXISTS hw2_test (
    id int, if1 float, if2 float, if3 float, if4 float, if5 float, if6 float, if7 float, if8 float, if9 float, if10 float, if11 float, if12 float, if13 float, cf1 string, cf2 string, cf3 string, cf4 string, cf5 string, cf6 string, cf7 string, cf8 string, cf9 string, cf10 string, cf11 string, cf12 string, cf13 string, cf14 string, cf15 string, cf16 string, cf17 string, cf18 string, cf19 string, cf20 string, cf21 string, cf22 string, cf23 string, cf24 string, cf25 string, cf26 string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
lines terminated by '\n'
location '/datasets/criteo/testdir';
