package PrefixSpanMethod;

/**
 * Created by prm14 on 2015/12/16.
 */


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.util.Map;

public class PrefixSpan_test {

    public static void main(String[] args) throws Exception {
        String inputFileName = args[0];
        double minSupport=Double.parseDouble(args[1]);
        int maxPatternLength=Integer.parseInt(args[2]);

        SparkConf sparkConf = new SparkConf().setAppName("PrefixSpanMethod");
        sparkConf.set("spark.ui.port", "5555");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        SQLContext sqlcontext = new SQLContext(ctx.sc());

        HiveContext hiveCtx = new HiveContext(ctx.sc());
        DataFrame df1 = hiveCtx.sql("select * from " + inputFileName);//读取数据，存入dataframe
        df1.printSchema();
        System.out.println(df1.count() + " lines data in total");
        DataFrame df2=df1.filter(df1.col("buy_cnt").gt(0))
                .select(df1.col("user_id"), df1.col("sku"), df1.col("dt"),df1.col("item_name"));
        System.out.println(df2.count() + " lines data after filtering");

        Map<String,String> sku2name=df2.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row s) throws Exception {
                return new Tuple2<String, String>(s.getAs("sku").toString(), s.getAs("item_name").toString());
            }
        }).collectAsMap();
        final Broadcast<Map<String, String>> s2n=ctx.broadcast(sku2name);

        //处理数据，生成<user,item>对的数据集，并以IBCF_input的javabean类对象格式存放，方便后面转换为dataFrame格式
        JavaRDD<PrefixSpan_input> input = df2.toJavaRDD().map(new Function<Row, PrefixSpan_input>() {
            @Override
            public PrefixSpan_input call(Row row) throws Exception {
                return new PrefixSpan_input()
                        .setUser(row.getAs("user_id").toString())
                        .setItem(row.getAs("sku").toString())
                        .setBehaviorTime(row.getAs("dt").toString());
            }
        });

        //创建输入的DataFrame，<user,item>两个字段
        DataFrame inputDF = sqlcontext.createDataFrame(input, PrefixSpan_input.class);
        PrefixSpan_train model= new PrefixSpan_train()
                .setMinSupport(minSupport)
                .setMaxPatternLength(maxPatternLength);
        DataFrame outputDF = model.run(sqlcontext, inputDF,s2n);
        outputDF.show();
        PrefixSpan_method.saveToHive(ctx, outputDF.toJavaRDD(), "pattern:String;times:String", "leyou_db.PrefixSpan_result_id_all");
        PrefixSpan_method.saveToHive(ctx, model.nameDF.toJavaRDD(), "pattern:String;times:String", "leyou_db.PrefixSpan_result_name_all");
    }
}