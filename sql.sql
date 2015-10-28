show tables;
create table if not exists prm_click as 
select user_id,item_id,time as t
from user_info
where behavior_type=1;