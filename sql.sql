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


select min(时间) as mint,max(时间) as maxt 
from [GL6].[dbo].[ZCS6]