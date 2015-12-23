package PrefixSpan;

/**
 * Created by prm14 on 2015/12/16.
 */


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.fpm.PrefixSpan;
import org.apache.spark.mllib.fpm.PrefixSpanModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class PrefixSpan_test {

    public static void main(String[] args) throws Exception {
        String inputFileName = args[0];
        final String behaviorType = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("PrefixSpan");
        sparkConf.set("spark.ui.port", "5555");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        SQLContext sqlcontext = new SQLContext(ctx.sc());

        JavaRDD<String> lines1 = ctx.textFile(inputFileName, 1);

        System.out.println("总共读取了" + lines1.count() + "条数据");
        //过滤出某种行为的数据
        JavaRDD<String> lines2 = lines1.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return !(s.split(",")[2].equals("0")||s.split(",")[2].equals("浏览数"));
            }
        });

        //处理数据，生成<user,item>对的数据集，并以IBCF_input的javabean类对象格式存放，方便后面转换为dataFrame格式
        JavaRDD<PrefixSpan_input> input = lines2.map(new Function<String, PrefixSpan_input>() {
            @Override
            public PrefixSpan_input call(String s) {
                String[] b = s.split(",");
                return new PrefixSpan_input().setUser(b[0]).setItem(b[1]).setBehaviorTime(b[5]);
            }
        });

        //创建输入的DataFrame，<user,item>两个字段
        DataFrame inputDF = sqlcontext.createDataFrame(input, PrefixSpan_input.class);

        JavaRDD<List<List<Integer>>> sequences = ctx.parallelize(Arrays.asList(
                Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3)),
                Arrays.asList(Arrays.asList(1), Arrays.asList(3, 2), Arrays.asList(1, 2)),
                Arrays.asList(Arrays.asList(1, 2), Arrays.asList(5)),
                Arrays.asList(Arrays.asList(6))
        ), 2);

        PrefixSpan prefixSpan = new PrefixSpan()
                .setMinSupport(0.5)
                .setMaxPatternLength(5);
        PrefixSpanModel<Integer> model = prefixSpan.run(sequences);
        for (PrefixSpan.FreqSequence<Integer> freqSeq : model.freqSequences().toJavaRDD().collect()) {
            System.out.println(freqSeq.javaSequence() + ", " + freqSeq.freq());
        }
    }
}