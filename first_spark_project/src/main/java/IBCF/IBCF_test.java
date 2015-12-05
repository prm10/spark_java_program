package IBCF;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * Created by prm14 on 2015/12/5.
 */
public class IBCF_test {

    public static void main(String[] args) throws Exception {//args:inputFileName,behaviorType,maxBehaviorTimes,maxCandidateSize
        if (args.length < 4) {
            System.err.println("输入参数不够，应该至少给出：输入数据文件名、行为类型、输入数据中用户最多行为次数、输出数据中候选集最多元素个数");
        }
        String inputFileName=args[0];
        final String behaviorType=args[1];
        int maxBehaviorTimes=Integer.parseInt(args[2]);
        int maxCandidateSize=Integer.parseInt(args[3]);

        SparkConf sparkConf = new SparkConf().setAppName("IBCF");
        sparkConf.set("spark.ui.port", "5555");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        SQLContext sqlcontext = new SQLContext(ctx.sc());

        JavaRDD<String> lines1 = ctx.textFile(inputFileName, 1);

        System.out.println(lines1.count() + " lines read in");
        //过滤出某种行为的数据
        JavaRDD<String> lines2 = lines1.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.split(",")[2].equals(behaviorType);
            }
        });

        //处理数据，生成<user,item>对的数据集，并以IBCF_input的javabean类对象格式存放，方便后面转换为dataFrame格式
        JavaRDD<IBCF_input> input = lines2.map(new Function<String, IBCF_input>() {
            @Override
            public IBCF_input call(String s) {
                String[] b = s.split(",");
                return new IBCF_input().setUser(b[0]).setItem(b[1]);
            }
        });

        //创建输入的DataFrame，<user,item>两个字段
        DataFrame inputDF = sqlcontext.createDataFrame(input, IBCF_input.class);
        IBCF ibcf = new IBCF()
                .setMaxBehaviorTimes(maxBehaviorTimes)
                .setMaxCandidateSize(maxCandidateSize);
        DataFrame outputDF=ibcf.run(ctx,sqlcontext,inputDF);
//        outputDF.show();
        int i = 0;
        for (Row show : outputDF.toJavaRDD().collect()) {
            System.out.println(show.getAs("item").toString());
            System.out.println("{" + show.getAs("itemList").toString() + "}");
            i = i + 1;
            if (i > 10) {
                break;
            }
        }
        ctx.stop();
    }
}
