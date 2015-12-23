package PrefixSpan;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.fpm.PrefixSpan;
import org.apache.spark.mllib.fpm.PrefixSpanModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by prm14 on 2015/12/19.
 */
public class PrefixSpan_train {
    private double minSupport;
    private int maxPatternLength;

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

//    public DataFrame run(JavaSparkContext ctx,SQLContext sqlcontext,DataFrame inputDF) {

//        JavaPairRDD<Long, Tuple2<Long,String>> uit = inputDF.toJavaRDD().mapToPair(new PairFunction<Row, Long, Tuple2<Long,String>>() {
//            @Override
//            public Tuple2<Long, Tuple2<Long,String>> call(Row row) throws Exception {
//                return new Tuple2<Long, Tuple2<Long,String>>(Long.parseLong(row.getAs("user").toString()),new Tuple2<Long, String>(Long.parseLong(row.getAs("item").toString()),String.valueOf(row.getAs("BehaviorTime"))));
//            }
//        });
//
//
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH");//小写的mm表示的是分钟
//        try {
//            Date behaviorTime = sdf.parse(b[5]);
//            return new PrefixSpan_input().setUser(b[0]).setItem(b[1]).setBehaviorTime(behaviorTime);
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
//
//        PrefixSpan prefixSpan = new PrefixSpan()
//                .setMinSupport(0.5)
//                .setMaxPatternLength(5);
//        PrefixSpanModel<Integer> model = prefixSpan.run(sequences);
//        for (PrefixSpan.FreqSequence<Integer> freqSeq : model.freqSequences().toJavaRDD().collect()) {
//            System.out.println(freqSeq.javaSequence() + ", " + freqSeq.freq());
//        }
//    }
}
