show tables;
create table if not exists prm_click as 
select user_id,item_id,time as t
from user_info
where behavior_type=1;


insert overwrite table prm14_result partition(ds='2015-11-21') select 1111,'prm14:100;asdfsd';

--高炉7
select dateStr,num from
(
	select dateStr,count(*) as num
	from 
	(
		SELECT CONVERT(varchar(100), [时间], 23) as dateStr
		FROM [GL7].[dbo].[ZCS7]
		where (热风压力<0.34
			or 冷风流量<20
			or 顶温东北>350
			or 富氧流量<5000
			)
			and 时间>'2014-01-25'
	) A
	group by dateStr
) B
where num>100
order by dateStr

--高炉6
select dateStr,num from
(
	select dateStr,count(*) as num
	from 
	(
		SELECT CONVERT(varchar(100), [时间], 23) as dateStr
		FROM [GL6].[dbo].[ZCS6]
		where 热风压力<0.28
			or 冷风流量<15
			or 顶温东北>400
			or 富氧流量<5000
	) A
	group by dateStr
) B
where num>100
order by dateStr

--高炉3
select dateStr,num from
(
	select dateStr,count(*) as num
	from 
	(
		SELECT CONVERT(varchar(100), [时间], 23) as dateStr
		FROM [GL3].[dbo].[ZCS3]
		where 热风压力<0.28
			or 冷风流量<15
			or 顶温东北>400
			or 富氧流量<4000
	) A
	group by dateStr
) B
where num>100
order by dateStr


select min(时间) as mint,max(时间) as maxt 
from [GL6].[dbo].[ZCS6]



-------------------------------------------------------------------
select * from leyou_db.ubcf_6to10 limit 10;

CREATE DATABASE prm;
use prm;

create table if not exists buy_info as 
select * 
from leyou_db.joint_feat_tb
where buy_cnt>0
;

drop table if exists user_count;
create table user_count as 
select user_id,count(*) as num
from buy_info
group by user_id
;

drop table if exists user_count_6to10;
create table user_count_6to10 as 
select user_id,count(*) as num
from buy_info
where dt<'"20151101"'
group by user_id
;

drop table if exists item_name;
create table item_name as 
select distinct sku as item_id,item_name
from leyou_db.joint_feat_tb
;

drop table if exists check_user;
create table check_user as 
select B.*,A.num
from
(select * from user_count_6to10 where num >10
) A
join
(select * from leyou_db.ubcf_6to10
) B
join
(select distinct user_id from buy_info
where dt>='"20151101"'
) C
join
(select distinct user_id from buy_info
where dt<'"20151101"'
) D
on A.user_id=B.user_id and B.user_id=C.user_id and D.user_id=C.user_id
;

--选择user："6a06697c2f0e596d61290245fe4d6560"      "86c6a6894bed1dfb9d1678674bce72d2":1.00000;"006c9430e023d6a6b23c7729d9023ae2":0.96263;"e324e1216112c6b6530f7dc751be1456":0.78974;"68e73c35ef47738730ad31c84f866388":0.78698;"e3ba265bf28e816704b67882e47186b0":0.53272;"b75831dee7f88058a1d2865951891681":0.45531;"b862c21e364d6c6c533bb302f5cc0ee1":0.42366;"455e833603097752fe5dbb6000f491ba":0.31527;"3eb98385c95f9bdeb81936d2ea34066d":0.15192        21

select dt,item_name from buy_info
where user_id='"6a06697c2f0e596d61290245fe4d6560"'
order by dt;














