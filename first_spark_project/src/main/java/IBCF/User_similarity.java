package IBCF;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

/**
 * Created by prm14 on 2016/1/23.
 */
public class User_similarity {

    public static void main(String[] args) throws Exception {//args:inputFileName,maxBehaviorTimes,maxCandidateSize
        if (args.length < 4) {
            System.err.println("输入参数不够，应该给出4个参数：输入数据文件名、行为类型、输入数据中用户最多行为次数、输出数据中候选集最多元素个数");
        }
        String inputFileName = args[0];
        int maxBehaviorTimes = Integer.parseInt(args[1]);
        int maxCandidateSize = Integer.parseInt(args[2]);

        SparkConf sparkConf = new SparkConf().setAppName("IBCF");
        sparkConf.set("spark.ui.port", "5555");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        SQLContext sqlcontext = new SQLContext(ctx.sc());

        DataFrame df = IBCF_method.getData(ctx, inputFileName);
        DataFrame lines2 = df.filter(df.col("buy_cnt").gt(0))
                .select(df.col("user_id"), df.col("sku"), df.col("dt"), df.col("item_name"));
        DataFrame trainSet = lines2.filter(lines2.col("dt").lt("\"20151101\""));
        DataFrame testSet = lines2.filter(lines2.col("dt").gt("\"20151031\""));

        System.out.println("剔除异常数据后还剩" + lines2.count() + "条数据，分为了训练集" + trainSet.count() + "条，测试集" + testSet.count() + "条");

        JavaRDD<IBCF_input> input = trainSet.toJavaRDD().map(new Function<Row, IBCF_input>() {
            @Override
            public IBCF_input call(Row s) {
                return new IBCF_input().setUser(s.getAs("sku").toString()).setItem(s.getAs("user_id").toString());
            }
        });

        //创建输入的DataFrame，<user,item>两个字段
        DataFrame inputDF = sqlcontext.createDataFrame(input, IBCF_input.class);
        IBCF model = new IBCF()
                .setMaxBehaviorTimes(maxBehaviorTimes)
                .setMaxCandidateSize(maxCandidateSize);
        DataFrame outputDF = model.run(ctx, sqlcontext, inputDF);
        outputDF.show();

//        JavaPairRDD<String,String> u_ilist=lines2.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
//            @Override
//            public Tuple2<String, String> call(Row row) throws Exception {
//                String user_id=row.getAs("user_id").toString();
//                String item_id=row.getAs("sku").toString();
//                String dt=row.getAs("dt").toString();
//                String item_name=row.getAs("item_name");
//                return new Tuple2<String, String>(user_id,item_id+"||"+item_name+"||"+dt);
//            }
//        }).reduceByKey(new Function2<String, String, String>() {
//            @Override
//            public String call(String s1, String s2) throws Exception {
//                return s1+"#"+s2;
//            }
//        });

        JavaPairRDD<String, String> resultF = outputDF.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                return new Tuple2<String, String>(row.getAs("item").toString(), row.getAs("itemList").toString());
            }
        });

        IBCF_method.saveToHive(ctx, IBCF_method.generateRow(resultF), "user_id:String;user_list:String", "leyou_db.ubcf_6to10");
        ctx.stop();
    }
}
