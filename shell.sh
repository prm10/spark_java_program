
scp ~/prm14-1.0.0.jar root@hadoopserver3:/home
ssh root\@hadoopserver3

cd /usr/hdp/2.3.2.0-2950/spark
./bin/spark-submit --class "datrain.item_based_CF" --master spark://192.168.1.50:7077 --executor-memory 20G /home/prm14-1.0.0.jar /tmp/prm_user1

su hdfs
hadoop fs -rm -r /tmp/prm_output
exit

spark-submit --class "org.apache.spark.examples.JavaWordCount" --master local[4] ~/Downloads/prm14-1.0.0.jar ~/Downloads/behavior.txt
spark-submit --class "datrain.item_based_CF" --master local[4] --executor-memory 2G ~/Downloads/prm14-1.0.0.jar ~/Downloads/behavior.txt
spark-submit --class "datrain.item_based_CF" --master local[4] prm14-1.0.0.jar behavior.txt

http://sparkbj38080.tunnel.yottabig.com:8000/