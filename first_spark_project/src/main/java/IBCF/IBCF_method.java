package IBCF;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by prm14 on 2016/1/4.
 */
public class IBCF_method {

    public static DataFrame getData(JavaSparkContext ctx,String tableName){
        HiveContext hiveCtx = new HiveContext(ctx.sc());
        DataFrame inputDF = hiveCtx.sql("select * from "+tableName);//读取数据，存入dataframe
        inputDF.printSchema();
        System.out.println("总共读入" + inputDF.count() + "行数据");
        return inputDF;
    }

    public static StructType gernerateStructType(String s){
        String[] kvs=s.split(";");
        List<StructField> fields = new ArrayList<StructField>();
        for(String x:kvs){
            String[] kv=x.split(":");
            String name=kv[0];
            String dataType=kv[1];
            if(dataType.equals("String")){
                fields.add(DataTypes.createStructField(name, DataTypes.StringType, true));
            }
            if(dataType.equals("Long")){
                fields.add(DataTypes.createStructField(name,DataTypes.LongType,true));
            }
        }
        return DataTypes.createStructType(fields);
    }

    public static JavaRDD<Row> generateRow(JavaPairRDD<String,String> x){
        return x.map(new Function<Tuple2<String, String>, Row>() {
            @Override
            public Row call(Tuple2<String, String> s) throws Exception {
                return RowFactory.create(s._1, s._2);
            }
        });
    }

    public static void saveToHive(JavaSparkContext ctx,JavaRDD<Row> outfile,String structType,String tableName){
        HiveContext hiveCtx = new HiveContext(ctx.sc());
        DataFrame result_df=hiveCtx.createDataFrame(outfile, gernerateStructType(structType));
        result_df.printSchema();
        result_df.registerTempTable("temp_table1");
        hiveCtx.sql("drop table if exists "+tableName);
        hiveCtx.sql("create external table if not exists "+tableName+" as select * from temp_table1");
    }

    public static void saveAsTextFile(JavaPairRDD<String,String> resultF,JavaPairRDD<String,String> n2n) throws Exception {
        //存为csv格式
        FileSystem hdfs=FileSystem.get(
                new java.net.URI("hdfs://mycluster"),
                new org.apache.hadoop.conf.Configuration());
        Path p=new Path("hdfs://mycluster/tmp/IBCF");
        if(hdfs.exists(p)){
            hdfs.delete(p,true);
        }
        p=new Path("hdfs://mycluster/tmp/IBCF_name");
        if(hdfs.exists(p)){
            hdfs.delete(p,true);
        }
        resultF.saveAsTextFile("/tmp/IBCF");
        n2n.saveAsTextFile("/tmp/IBCF_name");
    }

    public static DataFrame GetCandidateSet(JavaSparkContext ctx, DataFrame useritem,DataFrame item_itemlist,int topk){
        //将useritem转换为user-itemlist
        JavaPairRDD<String, String> useritemlist = useritem.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row row)throws Exception{
                return new Tuple2<String, String>(row.getAs("user").toString(), row.getAs("sku").toString());
            }
        }).reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String s1, String s2) throws Exception {
                return s1 + "," + s2;
            }
        });

        System.out.println("user-itemlist: " + useritemlist.count());

        //得到item-itemlist的map
        Map<String, String> item_itemlistmap = item_itemlist.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row row)throws Exception{
                return new Tuple2<String, String>(row.getAs("item").toString(),row.getAs("itemlist").toString());

            }
        }).collectAsMap();

        //广播出去，便于JavaRDD的各种闭包操作使用该变量
        final Broadcast<Map<String, String>> item_itemlistmap_brd = ctx.broadcast(item_itemlistmap);
        final int top_k = topk;
        //得到候选集
        JavaPairRDD<String, String> usercandidateitemlist = useritemlist.mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> tuple)throws Exception{

                String user = tuple._1();
                String[] itemlist = tuple._2().split(",");
                //最小堆排序
                Min_Heap heap = new Min_Heap(top_k);
                for (String item : itemlist) {
                    //如果在itemlist中没有该item
                    if(!(item_itemlistmap_brd.getValue().containsKey(item)))
                        continue;
                    //我写的是;号隔开
                    String[] candidateitemscores = item_itemlistmap_brd.getValue().get(item).split(";");

                    for (String canditemscore : candidateitemscores) {

                        String[] canditem_score = canditemscore.split(":");
                        String canditem = canditem_score[0];
                        Double score = Double.parseDouble(canditem_score[1]);
                        heap.add(canditem,score);
                    }
                }
                //排序
                heap.sort();
                if(heap.size == 0){
                    return null;
                }

                Min_Heap.kv item_entry = heap.result[0];
                String candidateitemlist = item_entry.key;//rec_item_id
                for (int i = 1; i < heap.size; i++) {
                    item_entry = heap.result[i];
                    candidateitemlist += "," + item_entry.key;
                }

                return new Tuple2<String, String>(user,candidateitemlist);

            }
        }).filter(new Function<Tuple2<String,String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String,String> tuple)throws Exception{
                if(tuple == null)
                    return false;
                return true;
            }
        });

        System.out.println("usercandiateitemlist: " + usercandidateitemlist.count());

        SQLContext sqlcontext = new SQLContext(ctx.sc());
        return sqlcontext.createDataFrame(IBCF_method.generateRow(usercandidateitemlist), IBCF_method.gernerateStructType("user:string;itemlist:string"));
    }

    public static Double[] GetPrecisionAndRecall(DataFrame candidateSet,DataFrame real_useritem){

        //将真实用户行为user-item对转换为user-itemlist的格式，并转换为以user为key的Map
        Map<String, String> real_useritemlistMap = real_useritem.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row row)throws Exception{
                return new Tuple2<String, String>(row.getAs("user").toString(),row.getAs("sku").toString());
            }
        }).reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String s1, String s2) throws Exception {
                return s1 + "," + s2;
            }
        }).collectAsMap();

        //候选集的user-itemlist的Map
        Map<String, String> candidatesetMap = candidateSet.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row row)throws Exception{
                return new Tuple2<String, String>(row.getAs("user").toString(),row.getAs("itemlist").toString());
            }
        }).collectAsMap();

        Double precision = 0.0;
        Double recall = 0.0;
        Long candidateitemnum = 0L;
        Long realitemnum = 0L;
        Long matchitemnum = 0L;
        //计算得到候选集和用户真实行为重合的user的行为数
        for (Map.Entry<String, String> entry : real_useritemlistMap.entrySet()){
            //感觉只计算有重合的user才比较合理
            if(candidatesetMap.containsKey(entry.getKey())){
                //遍历得到每个user的真实itemlist
                String[] itemlist = entry.getValue().split(",");
                //计算user对应的所有item数量
                realitemnum += itemlist.length;
            }
        }

        for (Map.Entry<String, String> entry : candidatesetMap.entrySet()){

            //得到当前user的值
            String user = entry.getKey();
            //得到实际该user的itemlist
            //如果不包括则继续循环，只计算有重合的user的行为，这样感觉比较合理
            if(!real_useritemlistMap.containsKey(user))
                continue;

            //遍历得到每个user的候选item
            String[] itemlist = entry.getValue().split(",");
            //计算当前user的候选item的个数，并add
            candidateitemnum += itemlist.length;
            String[] real_itemlist = real_useritemlistMap.get(user).split(",");
            //将实际该user的itemlist放入一个List中
            List<String> realitemList = new ArrayList<String>();
            for (String realitem : real_itemlist) {
                realitemList.add(realitem);
            }
            //遍历该user的候选item，计算在真实itemlist中命中的个数
            for (String item : itemlist) {
                if(realitemList.contains(item))
                    matchitemnum++;
            }
        }
        System.out.println("matchitemnum: " + matchitemnum);
        System.out.println("candidateitemnum: " + candidateitemnum);
        System.out.println("realitemnum: " + realitemnum);
        //准确率计算，user-item命中数占候选集user-item数量的比例(只计算候选集和用户真实行为中重合的user的行为)
        precision = matchitemnum*1.0/candidateitemnum;
        //召回率计算，user-item命中数占用户真实行为user-item数量的比例(只计算候选集和用户真实行为中重合的user的行为)
        recall = matchitemnum*1.0/realitemnum;
        Double[] evaluation = new Double[]{precision,recall};
        return evaluation;
    }
}
