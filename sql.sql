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
(select * from user_count_6to10 where num >3 and num <10
) A
join
(select * from leyou_db.ubcf_6to10
) B
on A.user_id=B.user_id
;

select * from check_user limit 10;

--选择user："dabb65e11aeb672d6f117a51bee1803d"      "f1b57413264d60f8a3769bb5912630b4":0.75593;"5852c9dc779f807f4578981cec2b2175":0.75593;"2b370d8d7686399350284cd36ef3ae0b":0.75593;"a7ec765d203b2fd6068ba9e2e974a666":0.75593;"ad182cf67f6888ef9dbe8075e44c54a4":0.75593;"63ef177863b5e10760bb694314ef982a":0.57143;"3c6c6e989db85d0206c6582bc649f159":0.53452;"ad3789f20ffdc6cd9cb8aa0630136ac9":0.53452;"6e3996863df0b45ac09db0132d4f6248":0.53452;"d092a1744ff51e42c6309f607957ecb1":0.53452

select dt,brew_cnt,buy_cnt,item_name from leyou_db.joint_feat_tb
where user_id='"5852c9dc779f807f4578981cec2b2175"'
order by dt;

dabb65e11aeb672d6f117a51bee1803d
"20150617"      0       1       "贝亲--奶瓶清洗剂400ml(新)"
"20150617"      0       1       "费雪牌--声光安抚海马-粉色"
"20150617"      0       1       "婴唯爱--天然玉米胚芽爽身粉100g"
"20150617"      1       1       "贝亲--浓缩型衣物柔软剂补充装500ml"
"20150617"      1       1       "晨光--可水洗水彩笔12色"
"20150617"      1       1       "贝亲--浓缩型衣物清洗剂补充装500ml"
"20150618"      0       1       "晨光--可水洗水彩笔12色"
"20151020"      1       1       "贝亲(喂养)--婴儿柔湿巾10*6片/包"
"20151020"      0       1       "贝亲(喂养)--婴儿柔湿巾25*4片/包"
"20151020"      1       1       "贝亲(喂养)--婴儿柔湿巾10*6片/包"
"20151020"      0       1       "惠步舒(新)--TXH20370机能鞋TXH20370灰14cm双"
"20151020"      0       1       "基诺浦(新)--TXG2023机能鞋TXG2023红13.5cm双"
"20151020"      0       1       "贝亲(喂养)--婴儿柔湿巾25*4片/包"
"20151020"      0       1       "惠步舒(新)--TXH20370机能鞋TXH20370灰14cm双"
"20151020"      0       1       "基诺浦(新)--TXG2023机能鞋TXG2023红13.5cm双"

f1b57413264d60f8a3769bb5912630b4
"20150618"      0       0       "好奇--金装超值装初生号NB70片"
"20150618"      0       1       "爱他美--4段幼儿配方奶粉(12-24月)800g/桶"
"20150618"      0       0       "贝亲(喂养)--婴儿抗菌洗衣皂120g*4块/包"
"20150707"      0       1       "贝亲(喂养)--婴儿抗菌洗衣皂120g*4块/包"
"20150707"      1       1       "贝亲(喂养)--婴儿柔湿巾10*6片/包"
"20150731"      0       0       "贝亲--防溢乳垫36片装(塑料袋装)"
"20150731"      0       0       "日康--婴儿专用剪刀"
"20150731"      0       0       "贝亲--婴儿柔湿巾80片3连包"
"20150731"      0       0       "贝亲--细轴棉棒180支"
"20150731"      0       0       "B&B--纤维洗涤剂(香草香)补充装1300ml"
"20151019"      0       0       "屁屁乐--专业护臀霜60G"
"20151019"      0       0       "贝亲--婴儿沐浴露200ml"
"20151019"      0       0       "贝亲--婴儿沐浴露200ml"
"20151019"      0       0       "屁屁乐--专业护臀霜60G"
"20151111"      0       1       "好奇--金装超值装初生号NB70片"
"20151111"      2       1       "花王--日本原装进口纸尿裤NB/90片(宝宝店专供)"
"20151111"      0       1       "贝亲--婴儿柔湿巾80片装"
"20151111"      0       0       "贝亲--婴儿柔湿巾80片3连包"
"20151111"      0       1       "好奇--金装超值装初生号NB70片"
"20151111"      2       1       "花王--日本原装进口纸尿裤NB/90片(宝宝店专供)"
"20151111"      0       0       "贝亲--婴儿柔湿巾80片3连包"
"20151111"      0       1       "贝亲--婴儿柔湿巾80片装"

5852c9dc779f807f4578981cec2b2175


