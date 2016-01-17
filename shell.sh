scp ~/PrefixSpan-1.0.0-jar-with-dependencies.jar root@hadoopserver3:/home
ssh root\@hadoopserver3
cd /usr/hdp/2.3.2.0-2950/spark
./bin/spark-submit \
--class "IBCF.IBCF_test" \
--master yarn --executor-memory 20G \
--total-executor-cores 48 \
/home/PrefixSpan-1.0.0-jar-with-dependencies.jar \
leyou_db.joint_feat_tb 1 1000 100

/tmp/shoppingcar.csv
leyou_db.joint_feat_tb


scp ~/PrefixSpan-1.0.0-jar-with-dependencies.jar root@hadoopserver3:/home
ssh root\@hadoopserver3
cd /usr/hdp/2.3.2.0-2950/spark
./bin/spark-submit \
--class "IBCF.IBCF_evaluate" \
--master yarn --executor-memory 20G \
--total-executor-cores 48 \
/home/PrefixSpan-1.0.0-jar-with-dependencies.jar \
leyou_db.joint_feat_tb leyou_db.ibcf_result_id_6to10 100


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



select * from tmalldb.prm14_result limit 10;





















庞人铭一：
输出结果一直无法成功写入Hive，故退而求其次研究生成DataFrame结构的输入输出。
找到了官网关于DataFrame的Document：https://spark.apache.org/docs/latest/sql-programming-guide.html#dataframes
发现按照官网示例"Inferring the Schema Using Reflection"用JavaBean生成DataFrame并不能成功，依据是inputDF.printSchema()发现只输出root，而没有如官网所示按JavaBean生成相应的结构。
庞人铭二：
在生成DataFrame的结果集的过程中发现，输出结果一直无法成功写入Hive不是连接hive阶段出错，而是生成的DataFrame有问题。
继续研究和尝试基于Java生成指定类型结构的DataFrame。
根据张媛要求，生成了协同过滤算法结果的截图和说明。
尝试将中间结果repartition成200个分区，发现对程序运行时间没有明显的影响。
庞人铭三：
根据官网生成DataFrame的另一个方案"Programmatically Specifying the Schema"成功生成指定类型结构的DataFrame，并将结果数据写入了Hive。
重构优化协同过滤算法的代码，将输入输出接口封装成函数。


public static class outfile_result implements Serializable {
	public Long item_id;
	public String item_list;

	outfile_result(Long i1,String list) {
		item_id=i1;
		item_list=list;
	}
}