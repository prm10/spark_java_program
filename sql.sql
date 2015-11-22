show tables;
create table if not exists prm_click as 
select user_id,item_id,time as t
from user_info
where behavior_type=1;


insert overwrite table prm14_result partition(ds='2015-11-21') select 1111,'prm14:100;asdfsd';