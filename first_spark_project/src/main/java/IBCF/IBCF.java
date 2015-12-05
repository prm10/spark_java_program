package IBCF;

/**
 * Created by prm14 on 2015/12/5.
 */
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.*;

/**
 * Created by prm14 on 2015/10/20.
 */

public class IBCF implements Serializable {
    private int maxBehaviorTimes=200;
    private int maxCandidateSize=100;

    public IBCF setMaxBehaviorTimes(int s) {
        maxBehaviorTimes = s;
        return this;
    }
    public IBCF setMaxCandidateSize(int s) {
        maxCandidateSize = s;
        return this;
    }
    public DataFrame run(JavaSparkContext ctx,SQLContext sqlcontext,DataFrame inputDF) {
        JavaPairRDD<Long, Long> user_behavior = inputDF.toJavaRDD().mapToPair(new PairFunction<Row, Long, Long>() {
            @Override
            public Tuple2<Long, Long> call(Row row) throws Exception {
                return new Tuple2<Long, Long>(Long.parseLong(row.getAs("user").toString()), Long.parseLong(row.getAs("item").toString()));
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
        final Broadcast<Map<Long, Long>> utbc=ctx.broadcast(user_times);

        System.out.println("共有用户" + user_times.size() + "个。");

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
                return s._2().size() < maxBehaviorTimes;
            }
        });//.repartition(200);
        System.out.println("形成了" + user_behaviors.count() + "个用户的行为数据。");

        //考虑热门user打压后，每个item对应的行为次数
        JavaPairRDD<Long, Double> item_times = ui_times.mapToPair(new PairFunction<Tuple2<Tuple2<Long, Long>, Long>, Long, Double>() {
            @Override
            public Tuple2<Long, Double> call(Tuple2<Tuple2<Long, Long>, Long> s) throws Exception {
                return new Tuple2<Long, Double>(s._1._2, (s._2() * s._2()) / Math.log(1 + utbc.getValue().get(s._1._1)));//item,count;
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
        JavaRDD<IBCF_output> outfile = user_behaviors.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Vector<Tuple2<Long, Long>>>, Tuple2<Long, Long>, Double>() {
            @Override
            public Iterable<Tuple2<Tuple2<Long, Long>, Double>> call(Tuple2<Long, Vector<Tuple2<Long, Long>>> ui) throws Exception {
                Long user_id = ui._1;
                Vector<Tuple2<Long, Long>> items = ui._2();
                List<Tuple2<Tuple2<Long, Long>, Double>> output = new ArrayList<Tuple2<Tuple2<Long, Long>, Double>>();
                for (int i1 = 0; i1 < items.size(); i1++) {
                    Tuple2<Long, Long> item1 = items.get(i1);
                    for (int i2 = i1 + 1; i2 < items.size(); i2++) {
                        Tuple2<Long, Long> item2 = items.get(i2);
                        double score = item1._2 * item2._2 / Math.log(1 + utbc.getValue().get(user_id));
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
        }).groupByKey().map(new Function<Tuple2<Long, Iterable<Tuple2<Long, Double>>>, IBCF_output>() {
            @Override
            public IBCF_output call(Tuple2<Long, Iterable<Tuple2<Long, Double>>> tuple) throws Exception {
                Long item1 = tuple._1();
                Min_Heap heap = new Min_Heap(maxCandidateSize);
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
                return new IBCF_output().setItem(String.valueOf(item1)).setItemList(item_list);
            }
        });
        System.out.println("生成推荐列表共" + outfile.count() + "个");
        return sqlcontext.createDataFrame(outfile, IBCF_output.class);
    }
}
