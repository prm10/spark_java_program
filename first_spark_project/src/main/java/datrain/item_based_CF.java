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

        //一次用户行为
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

        //记录每个user的行为列表
        JavaPairRDD<String, String> user_items = user_behavior.reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String s1, String s2) {
                return s1 +";"+ s2;
            }
        });

        //每个user的行为次数
        JavaPairRDD<String,Long> user_times= user_behavior.mapToPair(new PairFunction<Tuple2<String, String>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, String> s) throws Exception {
                long n=s._2().split(";").length;
                return new Tuple2<String, Long>(s._1(),n);
            }
        });

        //考虑热门user打压后，每个item对应的行为次数
        JavaPairRDD<String,Double> item_times=lines.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) {
                String[] b=SPACE.split(s);
                return new Tuple2<String, String>(b[0], b[1]);//user:item
            }
        }).join(user_times).mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Long>>, String, Double>() {
            @Override
            public Tuple2<String, Double> call(Tuple2<String, Tuple2<String, Long>> s) throws Exception {
                return new Tuple2<String, Double>(s._1(),1.0/s._2()._2());//item,count
            }
        }).reduceByKey(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double i1, Double i2) throws Exception {
                return i1 + i2;
            }
        });

        //生成item1：item2,count
        JavaRDD<Tuple2<String,Tuple2<String,Double>>> i1i2=user_items.join(user_times).flatMap(new FlatMapFunction<Tuple2<String, Tuple2<String, Long>>, Tuple2<String, Tuple2<String, Double>>>() {
            @Override
            public Iterable<Tuple2<String, Tuple2<String, Double>>> call(Tuple2<String, Tuple2<String, Long>> s) throws Exception {
                String[] items=s._2()._1().split(";");
                long user_times=s._2()._2();
                HashSet<String> itemSet=new HashSet<String>();
                List<Tuple2<String,Tuple2<String,Double>>> output=new ArrayList<Tuple2<String,Tuple2<String,Double>>>();
                for(int i1=0;i1<items.length;i1++){
                    String[] item1=items[i1].split(",");
                    if(!itemSet.contains(item1[0])){
                        itemSet.add(item1[0]);
                        ArrayList<String> info=new ArrayList<String>();
                        info.add("-1");
                        info.add(String.valueOf(items.length));
                        info.add("");
                        output.add(new Tuple2<String,ArrayList<String>>(item1[0],info));
                    }
                    for(int i2=i1+1;i2<items.length;i2++){
                        String[] item2=items[i2].split(",");
                        if(item2.equals(item1)){
                            continue;
                        }
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
        });

        //生成item1：item2,state,info
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
                                info.add("-1");
                                info.add(String.valueOf(items.length));
                                info.add("");
                                output.add(new Tuple2<String,ArrayList<String>>(item1[0],info));
                            }
                            for(int i2=i1+1;i2<items.length;i2++){
                                String[] item2=items[i2].split(",");
                                if(item2.equals(item1)){
                                    continue;
                                }
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
        //转换成键值对item1：item2,count,info
        JavaPairRDD<String,HashMap<String,ItemInfo>> item_items=item12.mapToPair(
                new PairFunction<Tuple2<String, ArrayList<String>>, String, HashMap<String,ItemInfo>>() {
                    @Override
                    public Tuple2<String, HashMap<String,ItemInfo>> call(Tuple2<String, ArrayList<String>> i12) throws Exception {
                        HashMap<String,ItemInfo> x1=new HashMap<String, ItemInfo>();
                        ArrayList<String> x2=new ArrayList<String>();
                        x2.add(i12._2().get(2));//info
                        long state=Long.parseLong(i12._2().get(1));//count
                        ItemInfo x3=new ItemInfo(i12._2().get(0),state,x2);
                        x1.put(i12._2().get(0),x3);
                        return new Tuple2<String, HashMap<String, ItemInfo>>(i12._1(),x1);
                    }
                }
        );

        //Reduce item1：item2,count,info
        JavaPairRDD<String,HashMap<String,ItemInfo>> item_items2=item_items.reduceByKey(
                new Function2<HashMap<String,ItemInfo>, HashMap<String,ItemInfo>, HashMap<String,ItemInfo>>() {
                    @Override
                    public HashMap<String,ItemInfo> call(HashMap<String,ItemInfo> v1, HashMap<String,ItemInfo> v2) throws Exception {
                        Iterator iter=v2.entrySet().iterator();
                        while(iter.hasNext()){
                            Map.Entry<String,ItemInfo> entry=(Map.Entry<String,ItemInfo>) iter.next();
                            String item2=entry.getKey();
                            ItemInfo info=entry.getValue();
                            if(v1.containsKey(item2)){
                                long n=v1.get(item2).count;
                                v1.put(item2,new ItemInfo(item2,n+info.count,info.info));
                            }
                            else{
                                v1.put(item2,new ItemInfo(info));
                            }
                        }
                        return v1;
                    }
                }
        );

        JavaPairRDD<String,HashMap<String,ItemInfo>> item_items3=item_items2.mapToPair(
                new PairFunction<Tuple2<String, HashMap<String, ItemInfo>>, String, HashMap<String, ItemInfo>>() {
                    @Override
                    public Tuple2<String, HashMap<String, ItemInfo>> call(Tuple2<String, HashMap<String, ItemInfo>> data) throws Exception {
                        String item1=data._1();
                        HashMap<String,ItemInfo> item2info=data._2();
                        HashMap<String,ItemInfo> output=new HashMap<String,ItemInfo>();
                        long n=item2info.get("-1").count;
                        Iterator iter=item2info.entrySet().iterator();
                        while(iter.hasNext()) {
                            Map.Entry<String, ItemInfo> entry = (Map.Entry<String, ItemInfo>) iter.next();
                            ItemInfo x=entry.getValue();
                            output.put(entry.getKey(),new ItemInfo(x.item2,x.count))
                        }
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
