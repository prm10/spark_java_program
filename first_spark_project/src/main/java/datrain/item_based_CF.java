package datrain;

import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.*;
import java.util.regex.Pattern;


/**
 * Created by prm14 on 2015/10/20.
 */

public final class item_based_CF {
    private static final Pattern SPACE = Pattern.compile(",");

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: item_based_CF <file>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("item_based_CF");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines0 = ctx.textFile(args[0], 1);
        System.out.println("总共读入" + lines0.count() + "行数据");
        //一次用户行为
        JavaRDD<String> lines=lines0.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                String[] b = SPACE.split(s);
                return Integer.parseInt(b[2])==1;
            }
        });
        JavaPairRDD<Long, String> user_behavior = lines.mapToPair(new PairFunction<String, Long, String>() {
            @Override
            public Tuple2<Long, String> call(String s) {
                String[] b = SPACE.split(s);
                StringBuilder c = new StringBuilder();
                for (int i = 1; i < b.length - 1; i++) {
                    c.append(b[i]);
                    c.append(',');
                }
                c.append(b[b.length - 1]);
                return new Tuple2<Long, String>(Long.parseLong(b[0]), c.toString());
            }
        }).cache();

        System.out.println("共有浏览行为" + user_behavior.count() + "条。");

        //每个user的行为次数
        JavaPairRDD<Long, Long> user_times = user_behavior.mapToPair(new PairFunction<Tuple2<Long, String>, Long, Long>() {
            @Override
            public Tuple2<Long, Long> call(Tuple2<Long, String> s) throws Exception {
                long n = s._2().split(";").length;
                return new Tuple2<Long, Long>(s._1(), n);
            }
        }).reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long l1, Long l2) throws Exception {
                return l1+l2;
            }
        });

        System.out.println("共有用户" + user_times.count() + "个。");

        //考虑热门user打压后，每个item对应的行为次数
        JavaPairRDD<Long, Double> item_times = lines.mapToPair(new PairFunction<String, Long, Long>() {
            @Override
            public Tuple2<Long, Long> call(String s) {
                String[] b = SPACE.split(s);
                return new Tuple2<Long, Long>(Long.parseLong(b[0]), Long.parseLong(b[1]));//user:item
            }
        }).join(user_times).mapToPair(new PairFunction<Tuple2<Long, Tuple2<Long, Long>>, Long, Double>() {
            @Override
            public Tuple2<Long, Double> call(Tuple2<Long, Tuple2<Long, Long>> s) throws Exception {
                return new Tuple2<Long, Double>(s._2()._1(), 1.0 / Math.log(1 + s._2()._2()));//item,count
            }
        }).reduceByKey(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double i1, Double i2) throws Exception {
                return i1 + i2;
            }
        }).cache();

        System.out.println("共有商品" + item_times.count() + "个。");

        //生成item1：item2,score
        JavaPairRDD<Long,String> outfile = user_behavior.reduceByKey(new Function2<String, String, String>() {
            @Override//将每个用户的行为连接起来
            public String call(String s1, String s2) {
                return s1 + ";" + s2;
            }
        }).filter(new Function<Tuple2<Long, String>, Boolean>() {
            @Override//去除行为过多的用户
            public Boolean call(Tuple2<Long, String> s) throws Exception {
                return (s._2().split(";").length < 100);
            }
        }).flatMapToPair(new PairFlatMapFunction<Tuple2<Long, String>, Long, Tuple2<Long, Double>>() {
            @Override//生成i1i2pair
            public Iterable<Tuple2<Long, Tuple2<Long, Double>>> call(Tuple2<Long, String> ui) throws Exception {
                String[] items = ui._2().split(";");
                List<Tuple2<Long, Tuple2<Long, Double>>> output = new ArrayList<Tuple2<Long, Tuple2<Long, Double>>>();
                for (int i1 = 0; i1 < items.length; i1++) {
                    String[] item1 = items[i1].split(",");
                    for (int i2 = i1 + 1; i2 < items.length; i2++) {
                        String[] item2 = items[i2].split(",");
                        if (item2[0].equals(item1[0])) {
                            continue;
                        }
                        output.add(new Tuple2<Long, Tuple2<Long, Double>>(Long.parseLong(item1[0]), new Tuple2<Long, Double>(Long.parseLong(item2[0]), 1 / Math.log(1 + items.length))));
                        output.add(new Tuple2<Long, Tuple2<Long, Double>>(Long.parseLong(item2[0]), new Tuple2<Long, Double>(Long.parseLong(item1[0]), 1 / Math.log(1 + items.length))));
                    }
                }
                return output;
            }
        }).groupByKey().mapToPair(new PairFunction<Tuple2<Long, Iterable<Tuple2<Long, Double>>>, Long, HashMap<Long, Double>>() {
            @Override//将i1:i2缩紧为i1：i2_list
            public Tuple2<Long, HashMap<Long, Double>> call(Tuple2<Long, Iterable<Tuple2<Long, Double>>> s) throws Exception {
                HashMap<Long, Double> out = new HashMap<Long, Double>();
                Long s1 = s._1;
                for (Tuple2<Long, Double> s3 : s._2()) {
                    Long item2 = s3._1();
                    double score = s3._2();
                    if (out.containsKey(item2)) {
                        out.put(item2, out.get(item2) + score);
                    } else
                        out.put(item2, score);
                }
                return new Tuple2<Long, HashMap<Long, Double>>(s1, out);
            }
        }).join(item_times).flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Tuple2<HashMap<Long, Double>, Double>>, Long, Tuple2<Long, Double>>() {
            @Override//除以i1的user_weight，并以i2:i1,score输出
            public Iterable<Tuple2<Long, Tuple2<Long, Double>>> call(Tuple2<Long, Tuple2<HashMap<Long, Double>, Double>> s) throws Exception {
                Long i1 = s._1();
                double weight=s._2._2;
                List<Tuple2<Long, Tuple2<Long, Double>>> out=new ArrayList<Tuple2<Long, Tuple2<Long, Double>>>();
                for(Map.Entry<Long, Double> h:s._2()._1().entrySet()){
                    out.add(new Tuple2<Long, Tuple2<Long, Double>>(h.getKey(),new Tuple2<Long, Double>(i1,h.getValue()/Math.sqrt(weight))));
                }
                return out;
            }
        }).groupByKey().mapToPair(new PairFunction<Tuple2<Long, Iterable<Tuple2<Long, Double>>>, Long, HashMap<Long, Double>>() {
            @Override//将i2:i1缩紧为i2：i1_list
            public Tuple2<Long, HashMap<Long, Double>> call(Tuple2<Long, Iterable<Tuple2<Long, Double>>> s) throws Exception {
                HashMap<Long, Double> out = new HashMap<Long, Double>();
                Long s1 = s._1;
                for (Tuple2<Long, Double> s3 : s._2()) {
                    Long item2 = s3._1();
                    double score = s3._2();
                    if (out.containsKey(item2)) {
                        out.put(item2, out.get(item2) + score);
                    } else
                        out.put(item2, score);
                }
                return new Tuple2<Long, HashMap<Long, Double>>(s1, out);
            }
        }).join(item_times).flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Tuple2<HashMap<Long, Double>, Double>>, Long, Tuple2<Long, Double>>() {
            @Override//除以i2的user_weight，并以i1:i2,score输出
            public Iterable<Tuple2<Long, Tuple2<Long, Double>>> call(Tuple2<Long, Tuple2<HashMap<Long, Double>, Double>> s) throws Exception {
                Long i1 = s._1();
                double weight=s._2._2;
                List<Tuple2<Long, Tuple2<Long, Double>>> out=new ArrayList<Tuple2<Long, Tuple2<Long, Double>>>();
                for(Map.Entry<Long, Double> h:s._2()._1().entrySet()){
                    out.add(new Tuple2<Long, Tuple2<Long, Double>>(h.getKey(),new Tuple2<Long, Double>(i1,h.getValue()/Math.sqrt(weight))));
                }
                return out;
            }
        }).groupByKey().mapToPair(new PairFunction<Tuple2<Long, Iterable<Tuple2<Long, Double>>>, Long, String>() {
            @Override//对结果排序
            public Tuple2<Long, String> call(Tuple2<Long, Iterable<Tuple2<Long, Double>>> tuple) throws Exception {
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
                return new Tuple2<Long, String>(item1,item_list);
            }
        });

        //对结果进行排序并输出
        System.out.println("生成item共"+outfile.count()+"个");
        outfile.saveAsTextFile("/tmp/prm_output");
//        List<Tuple2<String, String>> output = outfile.collect();
//        for (Tuple2<String, String> tuple : output) {
//            System.out.println("[" + tuple._1 + "]");
//            System.out.println(tuple._2);
//        }
        ctx.stop();
    }
}
