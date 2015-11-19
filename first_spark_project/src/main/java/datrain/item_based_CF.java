package datrain;


import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.*;
import java.util.regex.Pattern;
import java.util.Date;
import java.text.SimpleDateFormat;

/**
 * Created by prm14 on 2015/10/20.
 */

public final class item_based_CF {
    private static final Pattern SPACE = Pattern.compile(",");
    public static class outfile_result implements Serializable {
        public Long item_id;
        public String item_list;

        outfile_result(Long i1,String list) {
            item_id=i1;
            item_list=list;
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: item_based_CF <file>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("item_based_CF");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        //从文本读入数据
//        JavaRDD<String> lines0 = ctx.textFile(args[0], 1);
        //从hive读入数据
        HiveContext hiveCtx = new HiveContext(ctx.sc());
        hiveCtx.sql("use tmalldb");//指定数据库
        DataFrame lines=hiveCtx.sql("select * from user_info");//读取数据，存入dataframe

        System.out.println("总共读入" + lines.count() + "行数据");
        //一次用户行为：将lines转为RDD格式，并通过判断用户行为类别保留浏览行为。读入的Hive数据转为RDD后为JavaRDD<Row>，具体Row操作见下面示例
        JavaPairRDD<Long, Long> user_behavior=lines.toJavaRDD().filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row row) throws Exception {
                return Long.parseLong(row.getString(2))==1L;
            }
        }).mapToPair(new PairFunction<Row, Long, Long>() {
            @Override
            public Tuple2<Long, Long> call(Row row) throws Exception {
                return new Tuple2<Long, Long>(Long.parseLong(row.getString(0)),Long.parseLong(row.getString(1)));
            }
        });

        System.out.println("共有浏览行为" + user_behavior.count() + "条。");

        //每个user的行为次数
        final Map<Long, Long> user_times = user_behavior.mapToPair(new PairFunction<Tuple2<Long, Long>, Long, Long>() {
            @Override
            public Tuple2<Long, Long> call(Tuple2<Long, Long> s) throws Exception {
                return new Tuple2<Long, Long>(s._1,1L);
            }
        }).reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long l1, Long l2) throws Exception {
                return l1 + l2;
            }
        }).collectAsMap();
        ctx.broadcast(user_times);

        System.out.println("共有用户" + user_times.size() + "个。");

        //考虑热门user打压后，每个item对应的行为次数
        JavaPairRDD<Long, Double> item_times =user_behavior.mapToPair(new PairFunction<Tuple2<Long, Long>, Long, Double>() {
            @Override
            public Tuple2<Long, Double> call(Tuple2<Long, Long> s) throws Exception {
                return new Tuple2<Long, Double>(s._2, 1.0 / Math.log(1 + user_times.get(s._1)));//item,count;
            }
        }).reduceByKey(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double i1, Double i2) throws Exception {
                return i1 + i2;
            }
        });
//        ctx.broadcast(item_times);
        System.out.println("共有商品" + item_times.count() + "个。");

        //生成user:vector<item,times>
        JavaPairRDD<Long,Vector<Tuple2<Long,Long>>> user_behaviors = user_behavior.mapToPair(new PairFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Long>() {
            @Override
            public Tuple2<Tuple2<Long, Long>, Long> call(Tuple2<Long, Long> s) throws Exception {
                return new Tuple2<Tuple2<Long, Long>, Long>(s,1L);
            }
        }).reduceByKey(new Function2<Long, Long, Long>() {
            @Override//user,item:times
            public Long call(Long l1, Long l2) throws Exception {
                return l1+l2;
            }
        }).mapToPair(new PairFunction<Tuple2<Tuple2<Long, Long>, Long>, Long, Vector<Tuple2<Long, Long>>>() {
            @Override//user:vector<item,times>
            public Tuple2<Long, Vector<Tuple2<Long, Long>>> call(Tuple2<Tuple2<Long, Long>, Long> s) throws Exception {
                Vector<Tuple2<Long, Long>> x=new Vector<Tuple2<Long, Long>>();
                x.add(new Tuple2<Long, Long>(s._1._2,s._2()));
                return new Tuple2<Long, Vector<Tuple2<Long, Long>>>(s._1._1,x);
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
                return s._2().size()<100;
            }
        });
        System.out.println("形成了" + user_behaviors.count() + "个用户的行为数据。");

        //生成item1：item2,score
        JavaRDD<outfile_result> outfile=user_behaviors.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Vector<Tuple2<Long, Long>>>, Tuple2<Long, Long>, Double>() {
            @Override
            public Iterable<Tuple2<Tuple2<Long, Long>, Double>> call(Tuple2<Long, Vector<Tuple2<Long, Long>>> ui) throws Exception {
                Long user_id=ui._1;
                Vector<Tuple2<Long, Long>> items = ui._2();
                List<Tuple2<Tuple2<Long, Long>, Double>> output = new ArrayList<Tuple2<Tuple2<Long, Long>, Double>>();
                for (int i1 = 0; i1 < items.size(); i1++) {
                    Tuple2<Long, Long> item1 = items.get(i1);
                    for (int i2 = i1 + 1; i2 < items.size(); i2++) {
                        Tuple2<Long, Long> item2 = items.get(i2);
                        double score=item1._2*item2._2 / Math.log(1 + user_times.get(user_id));
                        output.add(new Tuple2<Tuple2<Long, Long>, Double>(new Tuple2<Long, Long>(item1._1,item2._1),score));
                        output.add(new Tuple2<Tuple2<Long, Long>, Double>(new Tuple2<Long, Long>(item2._1,item1._1),score));
                    }
                }
                return output;
            }
        }).reduceByKey(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double d1, Double d2) throws Exception {
                return d1+d2;
            }
        }).mapToPair(new PairFunction<Tuple2<Tuple2<Long, Long>, Double>, Long, Tuple2<Long, Double>>() {
            @Override//生成i1:i2,score
            public Tuple2<Long, Tuple2<Long, Double>> call(Tuple2<Tuple2<Long, Long>, Double> s) throws Exception {
                return new Tuple2<Long, Tuple2<Long, Double>>(s._1._1, new Tuple2<Long, Double>(s._1._2, s._2));
            }
        }).join(item_times).mapToPair(new PairFunction<Tuple2<Long, Tuple2<Tuple2<Long, Double>, Double>>, Long, Tuple2<Long, Double>>() {
            @Override
            public Tuple2<Long, Tuple2<Long, Double>> call(Tuple2<Long, Tuple2<Tuple2<Long, Double>, Double>> s) throws Exception {
                Long item1=s._1();
                Long item2=s._2._1._1;
                double score=s._2()._1()._2();
                double item_weight=s._2._2;
                return new Tuple2<Long, Tuple2<Long, Double>>(item2,new Tuple2<Long, Double>(item1,score/Math.sqrt(item_weight)));
            }
        }).join(item_times).mapToPair(new PairFunction<Tuple2<Long, Tuple2<Tuple2<Long, Double>, Double>>, Long, Tuple2<Long, Double>>() {
            @Override
            public Tuple2<Long, Tuple2<Long, Double>> call(Tuple2<Long, Tuple2<Tuple2<Long, Double>, Double>> s) throws Exception {
                Long item1=s._1();
                Long item2=s._2._1._1;
                double score=s._2()._1()._2();
                double item_weight=s._2._2;
                return new Tuple2<Long, Tuple2<Long, Double>>(item2,new Tuple2<Long, Double>(item1,score/Math.sqrt(item_weight)));
            }
        }).groupByKey().map(new Function<Tuple2<Long, Iterable<Tuple2<Long, Double>>>, outfile_result>() {
            @Override
            public outfile_result call(Tuple2<Long, Iterable<Tuple2<Long, Double>>> tuple) throws Exception {
                Long item1 = tuple._1();
                Min_Heap heap = new Min_Heap(100);
                for(Tuple2<Long, Double> tu:tuple._2()){
                    heap.add(String.valueOf(tu._1()), tu._2());
                }
                heap.sort();
                Min_Heap.kv item_entry = heap.result[0];
                String item_list = item_entry.key + ":" + item_entry.value;//rec_item_id,score
                for (int i = 1; i < heap.size; i++) {
                    item_entry = heap.result[i];
                    item_list += ";" + item_entry.key + ":" + item_entry.value;
                }
                return new outfile_result(item1,item_list);
            }
        });
//        JavaRDD<outfile_result> outfile = user_behavior.reduceByKey(new Function2<Long, Long, Long>() {
//            @Override//将每个用户的行为连接起来
//            public String call(Long s1, Long s2) {
//                return s1 + ";" + s2;
//            }
//        }).filter(new Function<Tuple2<Long, String>, Boolean>() {
//            @Override//去除行为过多的用户
//            public Boolean call(Tuple2<Long, String> s) throws Exception {
//                return (s._2().split(";").length < 500);
//            }
//        }).flatMapToPair(new PairFlatMapFunction<Tuple2<Long, String>, Long, Tuple2<Long, Double>>() {
//            @Override//生成i1i2pair
//            public Iterable<Tuple2<Long, Tuple2<Long, Double>>> call(Tuple2<Long, String> ui) throws Exception {
//                String[] items = ui._2().split(";");
//                List<Tuple2<Long, Tuple2<Long, Double>>> output = new ArrayList<Tuple2<Long, Tuple2<Long, Double>>>();
//                for (int i1 = 0; i1 < items.length; i1++) {
//                    String[] item1 = items[i1].split(",");
//                    for (int i2 = i1 + 1; i2 < items.length; i2++) {
//                        String[] item2 = items[i2].split(",");
//                        if (item2[0].equals(item1[0])) {
//                            continue;
//                        }
//                        output.add(new Tuple2<Long, Tuple2<Long, Double>>(Long.parseLong(item1[0]), new Tuple2<Long, Double>(Long.parseLong(item2[0]), 1 / Math.log(1 + items.length))));
//                        output.add(new Tuple2<Long, Tuple2<Long, Double>>(Long.parseLong(item2[0]), new Tuple2<Long, Double>(Long.parseLong(item1[0]), 1 / Math.log(1 + items.length))));
//                    }
//                }
//                return output;
//            }
//        }).groupByKey().mapToPair(new PairFunction<Tuple2<Long, Iterable<Tuple2<Long, Double>>>, Long, HashMap<Long, Double>>() {
//            @Override//将i1:i2缩紧为i1：i2_list
//            public Tuple2<Long, HashMap<Long, Double>> call(Tuple2<Long, Iterable<Tuple2<Long, Double>>> s) throws Exception {
//                HashMap<Long, Double> out = new HashMap<Long, Double>();
//                Long s1 = s._1;
//                for (Tuple2<Long, Double> s3 : s._2()) {
//                    Long item2 = s3._1();
//                    double score = s3._2();
//                    if (out.containsKey(item2)) {
//                        out.put(item2, out.get(item2) + score);
//                    } else
//                        out.put(item2, score);
//                }
//                return new Tuple2<Long, HashMap<Long, Double>>(s1, out);
//            }
//        }).join(item_times).flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Tuple2<HashMap<Long, Double>, Double>>, Long, Tuple2<Long, Double>>() {
//            @Override//除以i1的user_weight，并以i2:i1,score输出
//            public Iterable<Tuple2<Long, Tuple2<Long, Double>>> call(Tuple2<Long, Tuple2<HashMap<Long, Double>, Double>> s) throws Exception {
//                Long i1 = s._1();
//                double weight=s._2._2;
//                List<Tuple2<Long, Tuple2<Long, Double>>> out=new ArrayList<Tuple2<Long, Tuple2<Long, Double>>>();
//                for(Map.Entry<Long, Double> h:s._2()._1().entrySet()){
//                    out.add(new Tuple2<Long, Tuple2<Long, Double>>(h.getKey(),new Tuple2<Long, Double>(i1,h.getValue()/Math.sqrt(weight))));
//                }
//                return out;
//            }
//        }).groupByKey().mapToPair(new PairFunction<Tuple2<Long, Iterable<Tuple2<Long, Double>>>, Long, HashMap<Long, Double>>() {
//            @Override//将i2:i1缩紧为i2：i1_list
//            public Tuple2<Long, HashMap<Long, Double>> call(Tuple2<Long, Iterable<Tuple2<Long, Double>>> s) throws Exception {
//                HashMap<Long, Double> out = new HashMap<Long, Double>();
//                Long s1 = s._1;
//                for (Tuple2<Long, Double> s3 : s._2()) {
//                    Long item2 = s3._1();
//                    double score = s3._2();
//                    if (out.containsKey(item2)) {
//                        out.put(item2, out.get(item2) + score);
//                    } else
//                        out.put(item2, score);
//                }
//                return new Tuple2<Long, HashMap<Long, Double>>(s1, out);
//            }
//        }).join(item_times).flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Tuple2<HashMap<Long, Double>, Double>>, Long, Tuple2<Long, Double>>() {
//            @Override//除以i2的user_weight，并以i1:i2,score输出
//            public Iterable<Tuple2<Long, Tuple2<Long, Double>>> call(Tuple2<Long, Tuple2<HashMap<Long, Double>, Double>> s) throws Exception {
//                Long i1 = s._1();
//                double weight=s._2._2;
//                List<Tuple2<Long, Tuple2<Long, Double>>> out=new ArrayList<Tuple2<Long, Tuple2<Long, Double>>>();
//                for(Map.Entry<Long, Double> h:s._2()._1().entrySet()){
//                    out.add(new Tuple2<Long, Tuple2<Long, Double>>(h.getKey(),new Tuple2<Long, Double>(i1,h.getValue()/Math.sqrt(weight))));
//                }
//                return out;
//            }
//        }).groupByKey().map(new Function<Tuple2<Long, Iterable<Tuple2<Long, Double>>>, outfile_result>() {
//            @Override
//            public outfile_result call(Tuple2<Long, Iterable<Tuple2<Long, Double>>> tuple) throws Exception {
//                Long item1 = tuple._1();
//                Min_Heap heap = new Min_Heap(100);
//                for(Tuple2<Long, Double> tu:tuple._2()){
//                    heap.add(String.valueOf(tu._1()), tu._2());
//                }
//                heap.sort();
//                Min_Heap.kv item_entry = heap.result[0];
//                String item_list = item_entry.key + ":" + item_entry.value;//rec_item_id,score
//                for (int i = 1; i < heap.size; i++) {
//                    item_entry = heap.result[i];
//                    item_list += ";" + item_entry.key + ":" + item_entry.value;
//                }
//                return new outfile_result(item1,item_list);
//            }
//        });

        //对结果进行排序并输出
        System.out.println("生成i1i2pair共"+outfile.count()+"个");
//        存入文件
//        outfile.saveAsTextFile("/tmp/prm_output");
//        存入hive
        hiveCtx.sql("create external table if not exists prm14_item_based_CF_result(item_id bigint,item_list string) partitioned by (ds string)");
        hiveCtx.createDataFrame(outfile, outfile_result.class).registerTempTable("table1");
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        String ds=df.format(new Date());
        hiveCtx.sql("insert into table prm14_item_based_CF_result partition(ds='"+ds+"') select item_id,item_list from table1");

//        List<Tuple2<Long, String>> output = outfile.collect();
//        for (Tuple2<Long, String> tuple : output) {
//            System.out.println("[" + tuple._1 + "]");
//            System.out.println(tuple._2);
//        }
        ctx.stop();
    }
}
