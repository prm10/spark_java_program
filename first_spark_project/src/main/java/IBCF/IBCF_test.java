package IBCF;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import redis.clients.jedis.Jedis;
import scala.Tuple2;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by prm14 on 2015/12/5.
 */
public class IBCF_test {


    public static void main(String[] args) throws Exception {//args:inputFileName,behaviorType,maxBehaviorTimes,maxCandidateSize
        if (args.length < 4) {
            System.err.println("输入参数不够，应该给出4个参数：输入数据文件名、行为类型、输入数据中用户最多行为次数、输出数据中候选集最多元素个数");
        }
        String inputFileName = args[0];
        final String behaviorType = args[1];
        int maxBehaviorTimes = Integer.parseInt(args[2]);
        int maxCandidateSize = Integer.parseInt(args[3]);

        SparkConf sparkConf = new SparkConf().setAppName("IBCF");
        sparkConf.set("spark.ui.port", "5555");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        SQLContext sqlcontext = new SQLContext(ctx.sc());

//        HashMap<String, String> options = new HashMap<String, String>();
//        options.put("header", "true");
//        options.put("path", inputFileName);


//        JavaRDD<String> lines1 = ctx.textFile(inputFileName, 1);
//        System.out.println("总共读取了" + lines1.count() + "条数据");
        //过滤出某种行为的数据
//        JavaRDD<String> lines2 = lines1.filter(new Function<String, Boolean>() {
//            @Override
//            public Boolean call(String s) throws Exception {
////                return !(s.split(",").length<8||s.split(",")[2].equals("0")||s.split(",")[2].equals("浏览数"));
//                return !(s.split(",").length<8||s.split(",")[3].equals("0"));
//            }
//        });

        DataFrame df=IBCF_method.getData(ctx,inputFileName);
        DataFrame lines2=df.filter(df.col("buy_cnt").gt(0))
                .select(df.col("user_id"), df.col("sku"), df.col("dt"),df.col("item_name"));
        DataFrame trainSet=lines2.filter(lines2.col("dt").lt("\"20151101\""));
        DataFrame testSet=lines2.filter(lines2.col("dt").gt("\"20151031\""));



        System.out.println("剔除异常数据后还剩" + lines2.count() + "条数据，分为了训练集"+trainSet.count()+"条，测试集"+testSet.count()+"条");

        //处理数据，生成<user,item>对的数据集，并以IBCF_input的javabean类对象格式存放，方便后面转换为dataFrame格式
//        JavaRDD<IBCF_input> input = lines2.map(new Function<String, IBCF_input>() {
//            @Override
//            public IBCF_input call(String s) {
//                String[] b = s.split(",");
//                return new IBCF_input().setUser(b[0]).setItem(b[1]);
//            }
//        });
        JavaRDD<IBCF_input> input = trainSet.toJavaRDD().map(new Function<Row, IBCF_input>() {
            @Override
            public IBCF_input call(Row s) {
                return new IBCF_input().setUser(s.getAs("user_id").toString()).setItem(s.getAs("sku").toString());
            }
        });

        //创建输入的DataFrame，<user,item>两个字段
        DataFrame inputDF = sqlcontext.createDataFrame(input, IBCF_input.class);
        IBCF ibcf = new IBCF()
                .setMaxBehaviorTimes(maxBehaviorTimes)
                .setMaxCandidateSize(maxCandidateSize);
        DataFrame outputDF = ibcf.run(ctx, sqlcontext, inputDF);
        outputDF.show();

        JavaPairRDD<String,String> resultF=outputDF.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                return new Tuple2<String, String>(row.getAs("item").toString(),row.getAs("itemList").toString());
            }
        }).repartition(1);

        Map<String,String> sku2name=lines2.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row s) throws Exception {
//                String[] b = s.split(",");
                return new Tuple2<String, String>(s.getAs("sku").toString(), s.getAs("item_name").toString());
            }
        }).collectAsMap();
        final Broadcast<Map<String, String>> s2n=ctx.broadcast(sku2name);

        JavaPairRDD<String,String> n2n=outputDF.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                String item=row.getAs("item").toString();
                String[] itemList=row.getAs("itemList").toString().split(";");
                String result="\n推荐列表：\n";
                int i1=1;
                for(String i2:itemList){
                    String[] itemScore=i2.split(":");
                    result+="["+i1+":"+s2n.getValue().get(itemScore[0])+":"+itemScore[1]+"];\n";
                    i1++;
                }
                return new Tuple2<String, String>(s2n.getValue().get(item),result);
            }
        }).repartition(1);
        int i = 0;
        for (Tuple2<String,String> s:n2n.collect()){
            System.out.println(s._1+s._2+"\n");
            i = i + 1;
            if (i > 10) {
                break;
            }
        }

//        IBCF_method.saveAsTextFile(resultF,n2n);
        IBCF_method.saveToHive(ctx, IBCF_method.generateRow(resultF), "item_id:String;item_list:String", "leyou_db.ibcf_result_id_6to10");
        IBCF_method.saveToHive(ctx, IBCF_method.generateRow(n2n), "item_id:String;item_list:String", "leyou_db.ibcf_result_name_6to10");

        ////////////////////redis连接与操作//////////////////////////////
//        //连接redis
//        Jedis jedis = new Jedis("192.168.1.30", 6379);
//        //以<item,itemlist>的键值对存储
//        Map<String, String> resultmap = new HashMap<String, String>();
//        //DataFrame转换为JavaRDD格式数据
//        JavaRDD<Row> resultrdd = outputDF.toJavaRDD();
//        for (Row r : resultrdd.collect()) {
//            String item = r.getAs("item").toString();
//            String itemList = r.getAs("itemList").toString();
//            resultmap.put(item, itemList);
//        }
//        System.out.println("resultmap size: " + resultmap.size());
//        //将结果存入redis中
//        jedis.hmset("IBCF_resultlist", resultmap);
        ////////////////////redis连接与操作//////////////////////////////
        ctx.stop();
    }
}

