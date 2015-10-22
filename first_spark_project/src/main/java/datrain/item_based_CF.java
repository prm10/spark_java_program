package datrain;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

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
        JavaRDD<String> lines = ctx.textFile(args[0], 1);
        System.out.println("总共读入" + lines.count() + "行数据");
        //一次用户行为
        JavaPairRDD<String, String> user_behavior = lines.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) {
                String[] b = SPACE.split(s);
                StringBuilder c = new StringBuilder();
                for (int i = 1; i < b.length - 1; i++) {
                    c.append(b[i]);
                    c.append(',');
                }
                c.append(b[b.length - 1]);
                return new Tuple2<String, String>(b[0], c.toString());
            }
        }).cache();

        //每个user的行为次数
        JavaPairRDD<String, Long> user_times = user_behavior.mapToPair(new PairFunction<Tuple2<String, String>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, String> s) throws Exception {
                long n = s._2().split(";").length;
                return new Tuple2<String, Long>(s._1(), n);
            }
        });

        //考虑热门user打压后，每个item对应的行为次数
        JavaPairRDD<String, Double> item_times = lines.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) {
                String[] b = SPACE.split(s);
                return new Tuple2<String, String>(b[0], b[1]);//user:item
            }
        }).join(user_times).mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Long>>, String, Double>() {
            @Override
            public Tuple2<String, Double> call(Tuple2<String, Tuple2<String, Long>> s) throws Exception {
                return new Tuple2<String, Double>(s._1(), 1.0 / Math.log(1 + s._2()._2()));//item,count
            }
        }).reduceByKey(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double i1, Double i2) throws Exception {
                return i1 + i2;
            }
        }).cache();

        //生成item1：item2,score
        JavaRDD<Tuple2<String, Tuple2<String, Double>>> i1i2 = user_behavior.reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String s1, String s2) {
                return s1 + ";" + s2;
            }
        }).flatMap(new FlatMapFunction<Tuple2<String, String>, Tuple2<String, Tuple2<String, Double>>>() {
            @Override
            public Iterable<Tuple2<String, Tuple2<String, Double>>> call(Tuple2<String, String> ui) throws Exception {
                String[] items = ui._2().split(";");
                HashSet<String> itemSet = new HashSet<String>();
                List<Tuple2<String, Tuple2<String, Double>>> output = new ArrayList<Tuple2<String, Tuple2<String, Double>>>();
                for (int i1 = 0; i1 < items.length; i1++) {
                    String[] item1 = items[i1].split(",");
                    for (int i2 = i1 + 1; i2 < items.length; i2++) {
                        String[] item2 = items[i2].split(",");
                        if (item2[0].equals(item1[0])) {
                            continue;
                        }
                        output.add(new Tuple2<String, Tuple2<String, Double>>(item1[0], new Tuple2<String, Double>(item2[0], 1 / Math.log(1 + items.length))));
                        output.add(new Tuple2<String, Tuple2<String, Double>>(item2[0], new Tuple2<String, Double>(item1[0], 1 / Math.log(1 + items.length))));
                    }
                }
                return output;
            }
        });
        user_behavior.unpersist();

        //收集结果，并除以item热度
        JavaPairRDD<String, Iterable<Tuple2<String, Double>>> i1i2pair = i1i2.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Double>>, Tuple2<String, String>, Double>() {
            @Override
            public Tuple2<Tuple2<String, String>, Double> call(Tuple2<String, Tuple2<String, Double>> s) throws Exception {
                return new Tuple2<Tuple2<String, String>, Double>(new Tuple2<String, String>(s._1(), s._2()._1()), s._2()._2());
            }
        }).reduceByKey(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double s1, Double s2) throws Exception {
                return s1 + s2;
            }
        }).mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Double>, String, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Tuple2<String, Double>> call(Tuple2<Tuple2<String, String>, Double> s) throws Exception {
                return new Tuple2<String, Tuple2<String, Double>>(s._1()._1(), new Tuple2<String, Double>(s._1()._2(), s._2()));
            }
        }).join(item_times).mapToPair(new PairFunction<Tuple2<String, Tuple2<Tuple2<String, Double>, Double>>, String, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Tuple2<String, Double>> call(Tuple2<String, Tuple2<Tuple2<String, Double>, Double>> s) throws Exception {
                String i1 = s._1();
                String i2 = s._2()._1()._1();
                double score = s._2()._1()._2() / Math.sqrt(s._2()._2());
                return new Tuple2<String, Tuple2<String, Double>>(i2, new Tuple2<String, Double>(i1, score));
            }
        }).join(item_times).mapToPair(new PairFunction<Tuple2<String, Tuple2<Tuple2<String, Double>, Double>>, String, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Tuple2<String, Double>> call(Tuple2<String, Tuple2<Tuple2<String, Double>, Double>> s) throws Exception {
                String i1 = s._1();
                String i2 = s._2()._1()._1();
                double score = s._2()._1()._2() / Math.sqrt(s._2()._2());
                return new Tuple2<String, Tuple2<String, Double>>(i2, new Tuple2<String, Double>(i1, score));
            }
        }).groupByKey();

        //对结果进行排序并输出
        List<Tuple2<String, Iterable<Tuple2<String, Double>>>> output = i1i2pair.collect();
        for (Tuple2<String, Iterable<Tuple2<String, Double>>> tuple : output) {
            String item1 = tuple._1();
            Min_Heap heap = new Min_Heap(100);
            Iterator<Tuple2<String, Double>> iter = tuple._2().iterator();
            while (iter.hasNext()) {
                Tuple2<String, Double> tu = iter.next();
                heap.add(tu._1(), tu._2());
            }
            heap.sort();
            Min_Heap.kv item_entry = heap.result[0];
            String item_list = item_entry.key + ":" + item_entry.value;//rec_item_id,score
            for (int i = 1; i < heap.size; i++) {
                item_entry = heap.result[i];
                item_list += ";" + item_entry.key + ":" + item_entry.value;
            }
            System.out.println("[" + item1 + "]");
            System.out.println(item_list);
        }
        ctx.stop();
    }
}
