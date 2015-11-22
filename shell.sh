scp ~/prm14-1.0.0.jar root@hadoopserver3:/home
ssh root\@hadoopserver3
su hive
cd /usr/hdp/2.3.2.0-2950/spark
./bin/spark-submit --class "datrain.item_based_CF" --master yarn --executor-memory 20G --total-executor-cores 48 /home/prm14-1.0.0.jar /tmp/prm_user1

./bin/spark-submit --class "datrain.item_based_CF" --master spark://192.168.1.50:7077 --executor-memory 20G --total-executor-cores 48 /home/prm14-1.0.0.jar /tmp/prm_user1


su hdfs
hadoop fs -rm -r /tmp/prm_output
exit

spark-submit --class "org.apache.spark.examples.JavaWordCount" --master local[4] ~/Downloads/prm14-1.0.0.jar ~/Downloads/behavior.txt
spark-submit --class "datrain.item_based_CF" --master local[4] --executor-memory 2G ~/Downloads/prm14-1.0.0.jar ~/Downloads/behavior.txt
spark-submit --class "datrain.item_based_CF" --master local[4] prm14-1.0.0.jar behavior.txt

http://sparkbj38080.tunnel.yottabig.com:8000/

http://sparkbj.tunnel.yottabig.com:8000/

yarn application -kill application_1447840502449_0012



select * from tmalldb.prm14_result limit 10;





















庞人铭一：
改进协同过滤算法的输入输出接口，根据团队目标，采用DataFrame与Hive进行数据交互成功，这样方便上下游的程序进行数据接口的定义和交互；
熟悉Hive的操作，包括创建表和插入数据等操作，并在spark程序中执行sql语句成功。
庞人铭二：
根据李滔博士建议，对spark程序的三个方面进行优化：
1、将item的统计总和以broadcast的方式加载到所有worker，避免join操作；
2、用vector替换string，加速i1i2pair的生成
3、参考论文，对历史数据进行抽样后再给算法计算，减少运算量。
庞人铭三：
按上述要求继续优化代码：
对于第1个优化方向，发现item的统计总和item_times太大，broadcast到所有worker会内存溢出（提示需要190G的空间，显然不够。。），只好作罢；
对于第2个优化方向，已经优化，但是速度提升不明显；
对于第3个优化方向，由于需要读论文+改代码，工作量较大，准备作为下周目标。
此外，还写了一个将程序输入输出接口改为DataFrame的编程指南，已经上传至项目附件，供团队参考。


public static class outfile_result implements Serializable {
	public Long item_id;
	public String item_list;

	outfile_result(Long i1,String list) {
		item_id=i1;
		item_list=list;
	}
}