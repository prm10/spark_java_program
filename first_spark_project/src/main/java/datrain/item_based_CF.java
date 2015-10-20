package datrain;

import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

        JavaPairRDD<String,String> user_behavior=lines.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) {
                List<String> s2=Arrays.asList(SPACE.split(s));
                ArrayList<String> s3=new ArrayList<String>(s2);
                String user_id=s3.get(0);
                s3.remove(0);
                String.valueOf(s3);
                return new Tuple2<String, String>(user_id, s3.toString());
            }
        });

        JavaPairRDD<String, String> counts = user_behavior.reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String s1, String s2) {
                return s1 +";"+ s2;
            }
        });

        List<Tuple2<String, String>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        ctx.stop();
    }
}
