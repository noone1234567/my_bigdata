-- projects/2a/create_pred.hql
create table if not exists hw2_pred (
    id int,
    prediction float
)
row format delimited
fields terminated by '\t'
lines terminated by '\n'
stored as ORC
location 'noone1234567_hw2_pred';
