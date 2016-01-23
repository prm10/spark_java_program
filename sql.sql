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

drop table if exists item_name;
create table item_name as 
select distinct sku as item_id,item_name
from leyou_db.joint_feat_tb
;

drop table if exists check_user;
create table check_user as 
select B.*,A.num
from
(select * from user_count where num >5 and num <10
) A
join
(select * from leyou_db.ubcf_6to10
) B
on A.user_id=B.user_id
;

--选择user："8c93be24a8bb3fd0b284aa362f71e464" 及其相似userlist："43331a9efdd466572c4ef1a6b635ff2b":0.49593;"4474fd54fac38de3f8ff080c1cd833d4":0.45080;"efc6347f82b9ec6ef1cb29c9a18230f2":0.34795;"822b49a35b00c3170dfe58351ef33e0c":0.32808;"1e760b2ca309cbb1e160da1a28509c19":0.27074;"0f3196c6565b52403ad0d7bc77a569fd":0.24712;"9352368523b2770c7f9abd7ad4624817":0.24020;"0cdbb1297235096315f91a013066f5b1":0.23320;"98b68983f7b3502ef6f0252d52474dfc":0.23047;"ac34fd6649124a7e6db9991b2e21830e":0.1953

select user_id,item_name,dt from buy_info
where user_id='"8c93be24a8bb3fd0b284aa362f71e464"'
order by dt;














