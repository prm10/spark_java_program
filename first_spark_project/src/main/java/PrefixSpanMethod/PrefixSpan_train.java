package PrefixSpanMethod;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.fpm.PrefixSpan;
import org.apache.spark.mllib.fpm.PrefixSpanModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.util.*;

/**
 * Created by prm14 on 2015/12/19.
 */
public class PrefixSpan_train {
    private double minSupport;
    private int maxPatternLength;
    public DataFrame nameDF;

    public PrefixSpan_train setMinSupport(double s) {
        minSupport = s;
        return this;
    }

    public PrefixSpan_train setMaxPatternLength(int s) {
        maxPatternLength = s;
        return this;
    }

    public double getMinSupport() {
        return minSupport;
    }

    public int getMaxPatternLength() {
        return maxPatternLength;
    }

    public JavaRDD<List<List<String>>> changeFormat(DataFrame inputDF) {
        JavaPairRDD<String, Map<String, Set<String>>> tmp1 = inputDF.toJavaRDD().mapToPair(new PairFunction<Row, String, Map<String, Set<String>>>() {
            @Override
            public Tuple2<String, Map<String, Set<String>>> call(Row row) throws Exception {
                String user_id = row.getAs("user").toString();
                String item_id = row.getAs("item").toString();
                String t = row.getAs("behaviorTime").toString();
                Set<String> set = new HashSet<String>();
                set.add(item_id);
                Map<String, Set<String>> hm = new HashMap<String, Set<String>>();
                hm.put(t, set);
                return new Tuple2<String, Map<String, Set<String>>>(user_id, hm);//<user,<time,set(item)>>
            }
        }).reduceByKey(new Function2<Map<String, Set<String>>, Map<String, Set<String>>, Map<String, Set<String>>>() {
            @Override
            public Map<String, Set<String>> call(Map<String, Set<String>> s1, Map<String, Set<String>> s2) throws Exception {
                for (Map.Entry<String, Set<String>> s : s1.entrySet()) {
                    if (s2.containsKey(s.getKey())) {
                        s2.get(s.getKey()).addAll(s.getValue());
                    } else {
                        s2.put(s.getKey(), s.getValue());
                    }
                }
                return s2;
            }
        });
        return tmp1.map(new Function<Tuple2<String, Map<String, Set<String>>>, List<List<String>>>() {
            @Override
            public List<List<String>> call(Tuple2<String, Map<String, Set<String>>> s) throws Exception {
                List<List<String>> result = new ArrayList<List<String>>();
                for (Map.Entry<String, Set<String>> s1 : s._2.entrySet()) {
                    List<String> set2list = new ArrayList<String>(s1.getValue());
                    result.add(set2list);
                }
                return result;
            }
        });
    }

    public DataFrame run(SQLContext sqlcontext, DataFrame inputDF, final Broadcast<Map<String, String>> s2n) {
        JavaRDD<List<List<String>>> sequences = changeFormat(inputDF);
        PrefixSpan prefixSpan = new PrefixSpan()
                .setMinSupport(0.5)
                .setMaxPatternLength(5);
        PrefixSpanModel<String> model = prefixSpan.run(sequences);
        int i = 0;
        for (PrefixSpan.FreqSequence<String> freqSeq : model.freqSequences().toJavaRDD().collect()) {
            System.out.println(freqSeq.javaSequence() + ", " + freqSeq.freq());
            i++;
            if (i > 10) break;
        }

        JavaPairRDD<List<List<String>>, Long> tmp1 = model.freqSequences().toJavaRDD().mapToPair(new PairFunction<PrefixSpan.FreqSequence<String>, List<List<String>>, Long>() {
            @Override
            public Tuple2<List<List<String>>, Long> call(PrefixSpan.FreqSequence<String> s) throws Exception {
                return new Tuple2<List<List<String>>, Long>(s.javaSequence(), s.freq());
            }
        });

        JavaRDD<PrefixSpan_output> result = tmp1.map(new Function<Tuple2<List<List<String>>, Long>, PrefixSpan_output>() {
            @Override
            public PrefixSpan_output call(Tuple2<List<List<String>>, Long> s) throws Exception {
                String pattern = s._1.toString();
                Long times = s._2;
                return new PrefixSpan_output().setPattern(pattern).setTimes(times);
            }
        });

        JavaRDD<PrefixSpan_output> resultName = tmp1.map(new Function<Tuple2<List<List<String>>, Long>, PrefixSpan_output>() {
            @Override
            public PrefixSpan_output call(Tuple2<List<List<String>>, Long> s) throws Exception {
                String pattern = "\r\n";
                for (List<String> s1 : s._1) {
                    String r1 = "[";
                    for (String s2 : s1) {
                        r1 += s2n.getValue().get(s2) + ";";
                    }
                    r1 += "]";
                    pattern += r1 + "\r\n";
                }
                Long times = s._2;
                return new PrefixSpan_output().setPattern(pattern).setTimes(times);
            }
        });
        nameDF=sqlcontext.createDataFrame(resultName,PrefixSpan_output.class);
        return sqlcontext.createDataFrame(result, PrefixSpan_output.class);
    }
}
