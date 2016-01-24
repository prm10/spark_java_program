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
"151e9c1c6c2e2f94419988dd0c1ade97"      "981b067e9e5233825deb51fa0ae3a407":0.63960;"e5aff3539a6b168d84345ddb6ef9186e":0.51426;"9125dab95a3e57b9d53b6fd4591ffd82":0.45227;"e1030cf83116d845562b5fe5e91cece5":0.44998;"587f3d10647210017e9571185f7a4b17":0.38071;"da08b072f2898dc00733aa1b18eb0b80":0.36927;"e3bc310ebdc51f5a3babb6440d7c706a":0.36927;"bd636e8dad633bb9055816679e99629d":0.34816;"af13f856af7662e6853872ee610a6a35":0.34816;"edf4c5a5bd41847f17347633c2f85156":0.3385

select dt,brew_cnt,buy_cnt,item_name from leyou_db.joint_feat_tb
where user_id='"f1b57413264d60f8a3769bb5912630b4"'
order by dt;

案例一：喜欢买“贝亲--婴儿柔湿巾”的用户
dabb65e11aeb672d6f117a51bee1803d
"20150617"      1       0       "贝亲--奶瓶清洗剂400ml(新)"
"20150617"      1       0       "费雪牌--声光安抚海马-粉色"
"20150617"      1       0       "婴唯爱--天然玉米胚芽爽身粉100g"
"20150617"      1       1       "贝亲--浓缩型衣物柔软剂补充装500ml"
"20150617"      1       1       "晨光--可水洗水彩笔12色"
"20150617"      1       1       "贝亲--浓缩型衣物清洗剂补充装500ml"
"20150618"      1       0       "晨光--可水洗水彩笔12色"
"20151020"      1       1       "贝亲(喂养)--婴儿柔湿巾10*6片/包"
"20151020"      1       0       "贝亲(喂养)--婴儿柔湿巾25*4片/包"
"20151020"      1       1       "贝亲(喂养)--婴儿柔湿巾10*6片/包"
"20151020"      1       0       "惠步舒(新)--TXH20370机能鞋TXH20370灰14cm双"
"20151020"      1       0       "基诺浦(新)--TXG2023机能鞋TXG2023红13.5cm双"
"20151020"      1       0       "贝亲(喂养)--婴儿柔湿巾25*4片/包"
"20151020"      1       0       "惠步舒(新)--TXH20370机能鞋TXH20370灰14cm双"
"20151020"      1       0       "基诺浦(新)--TXG2023机能鞋TXG2023红13.5cm双"

f1b57413264d60f8a3769bb5912630b4
"20150618"      0       0       "好奇--金装超值装初生号NB70片"
"20150618"      1       0       "爱他美--4段幼儿配方奶粉(12-24月)800g/桶"
"20150618"      0       0       "贝亲(喂养)--婴儿抗菌洗衣皂120g*4块/包"
"20150707"      1       0       "贝亲(喂养)--婴儿抗菌洗衣皂120g*4块/包"
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
"20151111"      1       0       "好奇--金装超值装初生号NB70片"
"20151111"      1       2       "花王--日本原装进口纸尿裤NB/90片(宝宝店专供)"
"20151111"      1       0       "贝亲--婴儿柔湿巾80片装"
"20151111"      0       0       "贝亲--婴儿柔湿巾80片3连包"
"20151111"      1       0       "好奇--金装超值装初生号NB70片"
"20151111"      1       2       "花王--日本原装进口纸尿裤NB/90片(宝宝店专供)"
"20151111"      0       0       "贝亲--婴儿柔湿巾80片3连包"
"20151111"      1       0       "贝亲--婴儿柔湿巾80片装"


5852c9dc779f807f4578981cec2b2175
"20151007"      0       1       "歌瑞贝儿(新)--背袋熊立体袜/2双装GB130-212褐色12-24个月月包"
"20151007"      0       1       "贝亲—婴儿手口湿巾70片装(无酒精)"
"20151007"      0       1       "贝亲(喂养)--婴儿手口湿巾25*4片/包"
"20151007"      0       0       "歌瑞贝儿(新)--时尚条纹袜/2双装GB141-013L混色24-36个月月包"
"20151007"      1       2       "贝亲(喂养)--婴儿柔湿巾10*6片/包"
"20151007"      0       0       "歌瑞贝儿(新)--中性雪人毛巾袜GB154-029混色24-36个月包"
"20151007"      0       0       "歌瑞贝儿(新)--时尚条纹袜/2双装GB141-013L混色24-36个月月包"
"20151007"      0       1       "歌瑞贝儿(新)--男童条纹学步袜GB154-005混色6-12个月包"
"20151007"      0       1       "贝亲(喂养)--婴儿手口湿巾25*4片/包"
"20151007"      0       1       "歌瑞贝儿(新)--背袋熊立体袜/2双装GB130-212褐色12-24个月月包"
"20151007"      0       1       "贝亲—婴儿手口湿巾70片装(无酒精)"
"20151007"      0       1       "歌瑞贝儿(新)--男童条纹学步袜GB154-005混色6-12个月包"
"20151007"      0       0       "歌瑞贝儿(新)--中性雪人毛巾袜GB154-029混色24-36个月包"
"20151007"      1       2       "贝亲(喂养)--婴儿柔湿巾10*6片/包"

案例二：
151e9c1c6c2e2f94419988dd0c1ade97
"20150619"      0       0       "贝亲--自然实感宽口径玻璃奶瓶240ml(黄色)"
"20150619"      0       0       "贝亲(喂养)--新安抚奶嘴潜水艇(6-18月)1个"
"20150619"      0       0       "贝亲(喂养)--新安抚奶嘴小蓝鲸(3-6月)1个"
"20150619"      0       0       "津娃--迪娜娅浴盆L"
"20150623"      1       0       "贝亲--婴儿粉扑"
"20150623"      0       0       "贝亲--细轴棉棒180支"
"20150623"      0       0       "贝亲--耳孔清洁细轴棉棒"
"20150623"      1       0       "婴之侣--洗头帽"
"20150624"      1       0       "1500积分礼品--拼装益智积木玩具"
"20150624"      1       0       "歌瑞贝儿Great Baby(网)--单面提花皇冠短裤=Q131蓝90cm件"
"20150624"      1       0       "歌瑞凯儿Great Kid(网)--男童炫色图案背心（夏）GK15W-077蓝80件"
"20150624"      0       0       "歌瑞贝儿Great Baby(T)--40支双面布顽皮小恐龙对襟上衣GB133-2005VQ蓝80cmGB133-2005VQ蓝80CM件"
"20150624"      1       1       "歌瑞贝儿Great Baby(网)--莫代尔蜜蜂比尔可拆密档短裤=Q265蓝73cm件"
"20150624"      1       0       "歌瑞凯儿Great Kid(网)--男童炫色图案背心（夏）GK15W-077绿80件"
"20150624"      1       1       "歌瑞贝儿Great Baby(网)--莫代尔蜜蜂比尔背心=Q253蓝66cm件"
"20150624"      1       1       "歌瑞凯儿Great Kid(网)--男童炫色纯棉短裤（夏）GK15W-079蓝80件"
"20150624"      1       0       "歌瑞贝儿Great Baby(网)--圣麻提花单面布无骨缝短袖肩开套=Q58蓝80cm件"
"20150624"      1       0       "歌瑞凯儿Great Kid(网)--男童炫色纯棉五分裤（夏）GK15W-073灰80件"
"20150625"      1       0       "歌瑞贝儿Great Baby(网)--圣麻提花单面布无骨缝短袖套=Q61蓝110cm件"
"20150625"      1       0       "歌瑞贝儿Great Baby(网)--莫代尔蜜蜂比尔背心=Q247蓝66cm件"
"20150625"      1       0       "歌瑞贝儿Great Baby(网)--圣麻提花单面布无骨缝短袖肩开套=Q58蓝80cm件"
"20150626"      0       0       "贝亲--magmag鸭嘴式宝宝杯(绿色)"
"20150626"      0       0       "歌瑞贝儿Great Baby(网)--圣麻提花单面布无骨缝短袖套=Q61蓝110cm件"
"20150626"      1       1       "歌瑞贝儿Great Baby(网)--圣麻提花单面布无骨缝短袖肩开套=Q58蓝80cm件"
"20150626"      1       0       "津娃--儿童专用不锈钢餐碗(0岁以上)1个/盒"
"20150626"      0       0       "澳贝--好问爬行小蟹"
"20150626"      0       0       "歌瑞贝儿Great Baby(网)--单面提花皇冠短裤=Q131蓝90cm件"
"20150626"      0       0       "歌瑞凯儿Great Kid(网)--男童炫色纯棉短裤（夏）GK15W-079蓝120件"

981b067e9e5233825deb51fa0ae3a407
"20150604"      1       0       "歌瑞贝儿Great Baby(网)--莫代尔蜜蜂比尔背心=Q253蓝66cm件"
"20150604"      1       0       "歌瑞凯儿Great Kid(网)--男童炫色图案背心（夏）GK15W-077绿80件"
"20150604"      1       0       "歌瑞贝儿Great Baby(网)--莫代尔蜜蜂比尔背心=Q247蓝66cm件"
"20150604"      1       1       "歌瑞贝儿Great Baby(网)--圣麻提花单面布无骨缝短袖肩开套=Q58蓝80cm件"

e5aff3539a6b168d84345ddb6ef9186e
"20150707"      0       0       "歌瑞凯儿Great Kid(网)--女童炫色图案短袖T恤（夏）GK15W-068蓝90件"
"20150707"      1       0       "歌瑞贝儿Great Baby(网)--莫代尔蜜蜂比尔背心=Q247蓝66cm件"
"20150707"      0       0       "歌瑞凯儿Great Kid(网)--女童炫色图案短袖T恤（夏）GK15W-068黄90件"
"20150707"      1       0       "歌瑞凯儿Great Kid(网)--男童炫色纯棉五分裤（夏）GK15W-073灰80件"
"20150707"      2       1       "歌瑞凯儿Great Kid(网)--女童炫色梭织短裤（夏）GK15W-074咖啡80件"
"20150707"      1       1       "歌瑞贝儿Great Baby(网)--圣麻提花单面布无骨缝短袖肩开套=Q58蓝80cm件"
"20150707"      1       0       "歌瑞凯儿Great Kid(网)--男童炫色图案背心（夏）GK15W-077蓝80件"
"20150707"      0       0       "歌瑞凯儿Great Kid(网)--女童炫色纯棉背心两件装（夏）GK15W-088混色100件"
"20150707"      1       0       "歌瑞凯儿Great Kid(网)--女童炫色梭织短裤（夏）GK15W-074玫红90件"
"20150707"      1       1       "歌瑞凯儿Great Kid(网)--女童炫色图案短袖T恤（夏）GK15W-068蓝80件"
"20150707"      1       0       "歌瑞贝儿Great Baby(网)--莫代尔蜜蜂比尔背心=Q253蓝66cm件"
"20150707"      0       0       "歌瑞凯儿Great Kid(网)--女童炫色图案短袖T恤（夏）GK15W-068桃红90件"
"20150707"      2       2       "歌瑞凯儿Great Kid(网)--女童炫色图案短袖T恤（夏）GK15W-068桃红80件"
"20150707"      1       0       "歌瑞贝儿Great Baby(网)--莫代尔蜜蜂比尔可拆密档短裤=Q265蓝73cm件"
"20150707"      1       1       "歌瑞凯儿Great Kid(网)--女童炫色纯棉背心两件装（夏）GK15W-088混色80件"
"20150725"      0       0       "歌瑞凯儿Great Kid(网)--男童炫色条纹短袖T恤（夏）GK15W-069绿100件"
"20150725"      0       0       "歌瑞贝儿(新)--男生小熊三角内裤GB140-3002混色90CM包"
"20150725"      0       0       "歌瑞凯儿Great Kid(网)--女童炫色图案背心（夏）GK15W-090蓝100件"
"20150725"      0       0       "金盾婴宝--婴儿驱蚊手环(4条)"
"20150725"      0       0       "碧呵--艾草防蚊贴（36+6）片/盒"
"20150725"      0       0       "歌瑞贝儿(新)--男生小熊三角内裤GB140-3002混色90CM包"
"20150914"      0       0       "贝亲-标准口径塑料奶瓶(PP)240ML"
"20150914"      0       0       "贝亲—自然实感宽口径奶嘴(L)单个盒装"
"20150914"      0       0       "贝亲--标准奶嘴单个装L"

