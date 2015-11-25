package datrain;

import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.*;
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.*;
import java.util.regex.Pattern;
import java.util.Date;
import java.text.SimpleDateFormat;
import org.apache.spark.sql.RowFactory;

/**
 * Created by prm14 on 2015/10/20.
 */

public class item_based_CF {

//    public static class InputClass implements Serializable {
//        private Long user_id;
//        private Long item_id;
//        private Long behavior_type;
//
//        InputClass(Long s1, Long s2, Long s3) {
//            user_id = s1;
//            item_id = s2;
//            behavior_type=s3;
//        }
//    }
//
//    public static class OutputClass implements Serializable {
//        public Long item_id;
//        public String item_list;
//
//        OutputClass(Long i1, String list) {
//            item_id = i1;
//            item_list = list;
//        }
//    }

    public static StructType gernerateStructType(String s){
        String[] kvs=s.split(";");
        List<StructField> fields = new ArrayList<StructField>();
        for(String x:kvs){
            String[] kv=x.split(":");
            String name=kv[0];
            String dataType=kv[1];
            if(dataType.equals("String")){
                fields.add(DataTypes.createStructField(name,DataTypes.StringType,true));
            }
            if(dataType.equals("Long")){
                fields.add(DataTypes.createStructField(name,DataTypes.LongType,true));
            }
        }
        return DataTypes.createStructType(fields);
    }
    public static DataFrame getData(JavaSparkContext ctx,String way,String[] args,String structType){
        DataFrame inputDF=null;
        SQLContext sqlContext=new SQLContext(ctx);
        if(way.equals("hive")) {
            HiveContext hiveCtx = new HiveContext(ctx.sc());
            DataFrame df = hiveCtx.table("select user_id,item_id,behavior_type form tmalldb.user_info");//读取数据，存入dataframe
            df.printSchema();
            inputDF = sqlContext.createDataFrame(df.toJavaRDD(),gernerateStructType(structType));
        }
        else{
            JavaRDD<Row> lines0=ctx.textFile(args[0], 1).map(new Function<String, Row>() {
                @Override
                public Row call(String s) throws Exception {
                    String [] info=s.split(",");
                    return RowFactory.create(info[0],info[1],info[2]);
                }
            });
            inputDF = sqlContext.createDataFrame(lines0, gernerateStructType(structType));
        }
        inputDF.printSchema();
        System.out.println("总共读入" + inputDF.count() + "行数据");
        return inputDF;
    }

    public static void saveData(JavaSparkContext ctx,JavaRDD<Row> outfile,String structType){
        // 存入hive
        HiveContext hiveCtx = new HiveContext(ctx.sc());
        hiveCtx.sql("create external table if not exists prm14_result(item_id bigint,item_list string) partitioned by (ds string)");
        DataFrame result_df=hiveCtx.createDataFrame(outfile, gernerateStructType(structType));
        result_df.printSchema();
        result_df.registerTempTable("temp_table1");
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        String ds=df.format(new Date());
        hiveCtx.sql("insert overwrite table prm14_result partition(ds='"+ds+"') select * from temp_table1");
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: item_based_CF <file>");
        }

        SparkConf sparkConf = new SparkConf().setAppName("prm_item_based_CF");
        sparkConf.set("spark.ui.port", "5555");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        String structType="user_id:String;item_id:String;behavior_type:String";
        DataFrame inputDF=getData(ctx,"hive",args,structType);
        JavaPairRDD<Long, Long> user_behavior=inputDF.toJavaRDD().filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row row) throws Exception {
                return Long.parseLong(row.getString(2)) == 1L;
            }
        }).mapToPair(new PairFunction<Row, Long, Long>() {
            @Override
            public Tuple2<Long, Long> call(Row row) throws Exception {
                return new Tuple2<Long, Long>(Long.parseLong(row.getString(0)), Long.parseLong(row.getString(1)));
            }
        });
        System.out.println("共有浏览行为" + user_behavior.count() + "条。");

        //每个user的行为次数
        final Map<Long, Long> user_times = user_behavior.mapToPair(new PairFunction<Tuple2<Long, Long>, Long, Long>() {
            @Override
            public Tuple2<Long, Long> call(Tuple2<Long, Long> s) throws Exception {
                return new Tuple2<Long, Long>(s._1, 1L);
            }
        }).reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long l1, Long l2) throws Exception {
                return l1 + l2;
            }
        }).collectAsMap();
        ctx.broadcast(user_times);

        System.out.println("共有用户" + user_times.size() + "个。");

        //每个item的行为次数
//        final Map<Long, Long> item_num = user_behavior.mapToPair(new PairFunction<Tuple2<Long, Long>, Long, Long>() {
//            @Override
//            public Tuple2<Long, Long> call(Tuple2<Long, Long> s) throws Exception {
//                return new Tuple2<Long, Long>(s._2, 1L);
//            }
//        }).reduceByKey(new Function2<Long, Long, Long>() {
//            @Override
//            public Long call(Long l1, Long l2) throws Exception {
//                return l1 + l2;
//            }
//        }).filter(new Function<Tuple2<Long, Long>, Boolean>() {
//            @Override
//            public Boolean call(Tuple2<Long, Long> s) throws Exception {
//                return s._2 > 50L;
//            }
//        }).collectAsMap();
//
//        System.out.println("共有有效商品" + item_num.size() + "个。");

        JavaPairRDD<Tuple2<Long, Long>, Long> ui_times = user_behavior.mapToPair(new PairFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Long>() {
            @Override
            public Tuple2<Tuple2<Long, Long>, Long> call(Tuple2<Long, Long> s) throws Exception {
                return new Tuple2<Tuple2<Long, Long>, Long>(s, 1L);
            }
        }).reduceByKey(new Function2<Long, Long, Long>() {
            @Override//user,item:times
            public Long call(Long l1, Long l2) throws Exception {
                return l1 + l2;
            }
        });
        //生成user:vector<item,times>
        JavaPairRDD<Long, Vector<Tuple2<Long, Long>>> user_behaviors = ui_times.mapToPair(new PairFunction<Tuple2<Tuple2<Long, Long>, Long>, Long, Vector<Tuple2<Long, Long>>>() {
            @Override//user:vector<item,times>
            public Tuple2<Long, Vector<Tuple2<Long, Long>>> call(Tuple2<Tuple2<Long, Long>, Long> s) throws Exception {
                Vector<Tuple2<Long, Long>> x = new Vector<Tuple2<Long, Long>>();
                x.add(new Tuple2<Long, Long>(s._1._2, s._2()));
                return new Tuple2<Long, Vector<Tuple2<Long, Long>>>(s._1._1, x);
            }
        }).reduceByKey(new Function2<Vector<Tuple2<Long, Long>>, Vector<Tuple2<Long, Long>>, Vector<Tuple2<Long, Long>>>() {
            @Override
            public Vector<Tuple2<Long, Long>> call(Vector<Tuple2<Long, Long>> s1, Vector<Tuple2<Long, Long>> s2) throws Exception {
                s1.addAll(s2);
                return s1;
            }
        }).filter(new Function<Tuple2<Long, Vector<Tuple2<Long, Long>>>, Boolean>() {
            @Override//去除行为过多的用户
            public Boolean call(Tuple2<Long, Vector<Tuple2<Long, Long>>> s) throws Exception {
                return s._2().size() < 100;
            }
        });//.repartition(200);
        System.out.println("形成了" + user_behaviors.count() + "个用户的行为数据。");

        //考虑热门user打压后，每个item对应的行为次数
        JavaPairRDD<Long, Double> item_times = ui_times.mapToPair(new PairFunction<Tuple2<Tuple2<Long, Long>, Long>, Long, Double>() {
            @Override
            public Tuple2<Long, Double> call(Tuple2<Tuple2<Long, Long>, Long> s) throws Exception {
                return new Tuple2<Long, Double>(s._1._2, (s._2() * s._2()) / Math.log(1 + user_times.get(s._1._1)));//item,count;
            }
        }).reduceByKey(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double i1, Double i2) throws Exception {
                return i1 + i2;
            }
        });
//        ctx.broadcast(item_times);
        System.out.println("共有商品" + item_times.count() + "个。");

        //生成item1：item2,score
        JavaRDD<Row> outfile = user_behaviors.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Vector<Tuple2<Long, Long>>>, Tuple2<Long, Long>, Double>() {
            @Override
            public Iterable<Tuple2<Tuple2<Long, Long>, Double>> call(Tuple2<Long, Vector<Tuple2<Long, Long>>> ui) throws Exception {
                Long user_id = ui._1;
                Vector<Tuple2<Long, Long>> items = ui._2();
                List<Tuple2<Tuple2<Long, Long>, Double>> output = new ArrayList<Tuple2<Tuple2<Long, Long>, Double>>();
                for (int i1 = 0; i1 < items.size(); i1++) {
                    Tuple2<Long, Long> item1 = items.get(i1);
                    for (int i2 = i1 + 1; i2 < items.size(); i2++) {
                        Tuple2<Long, Long> item2 = items.get(i2);
                        double score = item1._2 * item2._2 / Math.log(1 + user_times.get(user_id));
                        output.add(new Tuple2<Tuple2<Long, Long>, Double>(new Tuple2<Long, Long>(item1._1, item2._1), score));
                        output.add(new Tuple2<Tuple2<Long, Long>, Double>(new Tuple2<Long, Long>(item2._1, item1._1), score));
                    }
                }
                return output;
            }
        }).reduceByKey(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double d1, Double d2) throws Exception {
                return d1 + d2;
            }
        }).mapToPair(new PairFunction<Tuple2<Tuple2<Long, Long>, Double>, Long, Tuple2<Long, Double>>() {
            @Override//生成i1:i2,score
            public Tuple2<Long, Tuple2<Long, Double>> call(Tuple2<Tuple2<Long, Long>, Double> s) throws Exception {
                return new Tuple2<Long, Tuple2<Long, Double>>(s._1._1, new Tuple2<Long, Double>(s._1._2, s._2));
            }
        }).join(item_times).mapToPair(new PairFunction<Tuple2<Long, Tuple2<Tuple2<Long, Double>, Double>>, Long, Tuple2<Long, Double>>() {
            @Override
            public Tuple2<Long, Tuple2<Long, Double>> call(Tuple2<Long, Tuple2<Tuple2<Long, Double>, Double>> s) throws Exception {
                Long item1 = s._1();
                Long item2 = s._2._1._1;
                double score = s._2()._1()._2();
                double item_weight = s._2._2;
                return new Tuple2<Long, Tuple2<Long, Double>>(item2, new Tuple2<Long, Double>(item1, score / Math.sqrt(item_weight)));
            }
        }).join(item_times).mapToPair(new PairFunction<Tuple2<Long, Tuple2<Tuple2<Long, Double>, Double>>, Long, Tuple2<Long, Double>>() {
            @Override
            public Tuple2<Long, Tuple2<Long, Double>> call(Tuple2<Long, Tuple2<Tuple2<Long, Double>, Double>> s) throws Exception {
                Long item1 = s._1();
                Long item2 = s._2._1._1;
                double score = s._2()._1()._2();
                double item_weight = s._2._2;
                return new Tuple2<Long, Tuple2<Long, Double>>(item2, new Tuple2<Long, Double>(item1, score / Math.sqrt(item_weight)));
            }
        }).groupByKey().map(new Function<Tuple2<Long, Iterable<Tuple2<Long, Double>>>, Row>() {
            @Override
            public Row call(Tuple2<Long, Iterable<Tuple2<Long, Double>>> tuple) throws Exception {
                Long item1 = tuple._1();
                Min_Heap heap = new Min_Heap(100);
                for (Tuple2<Long, Double> tu : tuple._2()) {
                    heap.add(String.valueOf(tu._1()), tu._2());
                }
                heap.sort();
                Min_Heap.kv item_entry = heap.result[0];
                DecimalFormat format = new DecimalFormat("0.00000");
                String item_list = item_entry.key + ":" + format.format(item_entry.value);//rec_item_id,score
                for (int i = 1; i < heap.size; i++) {
                    item_entry = heap.result[i];
                    item_list += ";" + item_entry.key + ":" + format.format(item_entry.value);
                }
                return RowFactory.create(item1, item_list);
            }
        });
        System.out.println("生成推荐列表共" + outfile.count() + "个");

        structType="item_id:Long;item_list:String";
        saveData(ctx,outfile,structType);

//        int i = 0;
//        for (Row show : outfile.collect()) {
//            System.out.println(show.getLong(0));
//            System.out.println("{" + show.getString(1) + "}");
//            i = i + 1;
//            if (i > 100) {
//                break;
//            }
//        }
        ctx.stop();
    }
}
