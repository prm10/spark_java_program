package datrain;

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

        JavaPairRDD<String,String> user_behavior=lines.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) {
                String[] b=SPACE.split(s);
                StringBuilder c=new StringBuilder();
                for(int i=1;i<b.length-1;i++){
                    c.append(b[i]);
                    c.append(',');
                }
                c.append(b[b.length - 1]);
                return new Tuple2<String, String>(b[0], c.toString());
            }
        });

        JavaPairRDD<String, String> user_items = user_behavior.reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String s1, String s2) {
                return s1 +";"+ s2;
            }
        });

        JavaRDD<Tuple2<String,ArrayList<String>>> item12=user_items.flatMap(
                new FlatMapFunction<Tuple2<String, String>, Tuple2<String, ArrayList<String>>>() {
                    @Override
                    public Iterable<Tuple2<String, ArrayList<String>>> call(Tuple2<String, String> ui) throws Exception {
                        String[] items=ui._2().split(";");
                        HashSet<String> itemSet=new HashSet<String>();
                        List<Tuple2<String,ArrayList<String>>> output=new ArrayList<Tuple2<String, ArrayList<String>>>();
                        for(int i1=0;i1<items.length;i1++){
                            String[] item1=items[i1].split(",");
                            if(!itemSet.contains(item1[0])){
                                itemSet.add(item1[0]);
                                ArrayList<String> info=new ArrayList<String>();
                                info.add(String.valueOf(items.length));
                                info.add("-1");
                                info.add("");
                                output.add(new Tuple2<String,ArrayList<String>>(item1[0],info));
                            }
                            for(int i2=i1+1;i2<items.length;i2++){
                                String[] item2=items[i2].split(",");
                                ArrayList<String> info1=new ArrayList<String>();
                                info1.add(item2[0]);
                                info1.add("1");
                                info1.add(items[i1]+";"+items[i2]);
                                output.add(new Tuple2<String,ArrayList<String>>(item1[0],info1));
                                ArrayList<String> info2=new ArrayList<String>();
                                info2.add(item1[0]);
                                info2.add("1");
                                info2.add(items[i2]+";"+items[i1]);
                                output.add(new Tuple2<String,ArrayList<String>>(item2[0],info2));
                            }
                        }
                        return output;
                    }
                }
        );
        JavaPairRDD<String,ArrayList<String>> item_items=item12.mapToPair(
                new PairFunction<Tuple2<String, ArrayList<String>>, String, ArrayList<String>>() {
                    @Override
                    public Tuple2<String, ArrayList<String>> call(Tuple2<String, ArrayList<String>> i12) throws Exception {
                        return i12;
                    }
                }
        );

        JavaPairRDD<String,ArrayList<String>> item_items2=user_items.reduceByKey(
                new Function2<ArrayList<String>, ArrayList<String>, ArrayList<String>>() {
                    @Override
                    public ArrayList<String> call(ArrayList<String> v1, ArrayList<String> v2) throws Exception {
                        return null;
                    }
                }
        );

//        List<Tuple2<String, String>> output = user_items.collect();
//        for (Tuple2<?,?> tuple : output) {
//            System.out.println(tuple._1() + ": " + tuple._2());
//        }
        ctx.stop();
    }
}
