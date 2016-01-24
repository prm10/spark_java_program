#生成user-based cf
scp ~/PrefixSpan-1.0.0.jar root@hadoopserver3:/home
ssh root\@hadoopserver3
cd /usr/hdp/2.3.2.0-2950/spark
./bin/spark-submit \
--class "IBCF.User_similarity" \
--master yarn --executor-memory 20G \
--total-executor-cores 48 \
/home/PrefixSpan-1.0.0.jar \
leyou_db.joint_feat_tb 2000 10

#生成候选集
scp ~/PrefixSpan-1.0.0.jar root@hadoopserver3:/home
ssh root\@hadoopserver3
cd /usr/hdp/2.3.2.0-2950/spark
./bin/spark-submit \
--class "IBCF.IBCF_test" \
--master yarn --executor-memory 20G \
--total-executor-cores 48 \
/home/PrefixSpan-1.0.0.jar \
leyou_db.joint_feat_tb 1 2000 100

#离线测评
scp ~/PrefixSpan-1.0.0.jar root@hadoopserver3:/home
ssh root\@hadoopserver3
cd /usr/hdp/2.3.2.0-2950/spark
./bin/spark-submit \
--class "IBCF.IBCF_evaluate" \
--master yarn --executor-memory 20G \
--total-executor-cores 48 \
/home/PrefixSpan-1.0.0.jar \
leyou_db.joint_feat_tb leyou_db.ibcf_result_id_6to10 100

#调用PrefixSpan
scp ~/PrefixSpan-1.0.0.jar root@hadoopserver3:/home
ssh root\@hadoopserver3
cd /usr/hdp/2.3.2.0-2950/spark
./bin/spark-submit \
--class "PrefixSpanMethod.PrefixSpan_test" \
--master yarn --executor-memory 20G \
--total-executor-cores 48 \
/home/PrefixSpan-1.0.0.jar \
leyou_db.joint_feat_tb 1e-4 10


/tmp/shoppingcar.csv
leyou_db.joint_feat_tb

su hive

./bin/spark-submit --class "datrain.item_based_CF" --master yarn --executor-memory 20G --total-executor-cores 48 /home/prm14-1.0.0.jar /tmp/prm_user1



./bin/spark-submit --class "datrain.item_based_CF" --master spark://192.168.1.50:7077 --executor-memory 20G --total-executor-cores 48 /home/prm14-1.0.0.jar /tmp/prm_user1


su hdfs
hadoop fs -rm -r /tmp/prm_output
exit

spark-submit --class "org.apache.spark.examples.JavaWordCount" --master local[4] ~/Downloads/prm14-1.0.0.jar ~/Downloads/behavior.txt
spark-submit --class "datrain.item_based_CF" --master local[4] --executor-memory 2G ~/Downloads/prm14-1.0.0.jar ~/Downloads/behavior.txt
spark-submit --class "datrain.item_based_CF" --master local[4] prm14-1.0.0.jar behavior.txt


http://yarn.tunnel.yottabig.com:8000/cluster

yarn application -kill application_1447840502449_0012

sql
----------------------------------------------------------------------------------------------------------------------
select * from tmalldb.prm14_result limit 10;

select * from leyou_db.PrefixSpan_result_name_all limit 100;
select pattern from leyou_db.PrefixSpan_result_name_all limit 10;
----------------------------------------------------------------------------------------------------------------------
一、
生成user-based CF的结果，储存于leyou_db.ubcf_6to10中。
通过user找相似user（user-based CF），发现user行为太稀疏了，每个user一般只对4个以内的item有行为。即使是通过行为次数很多的user找相似user，对应的相似的user的行为次数也只有3~4个，所以没有看出特别的规律
二、

三、
生成了PrefixSpan算法的结果表leyou_db.PrefixSpan_result_id_all和leyou_db.PrefixSpan_result_name_all，表的格式为：常见模式pattern|模式中商品个数length|该模式出现的次数times

