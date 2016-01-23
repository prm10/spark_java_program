package IBCF;

/**
 * Created by prm14 on 2016/1/13.
 */

import java.io.Serializable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class IBCF_evaluate implements Serializable{


    public static void main(String[] args) throws Exception {
        //3 args:inputFileName,IBCFtableName,topk
        String inputFileName = args[0];
        String IBCFtableName = args[1];
        int topk = Integer.parseInt(args[2]);

        SparkConf sparkConf = new SparkConf().setAppName("IBCF");
        sparkConf.set("spark.ui.port", "5555");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        SQLContext sqlcontext = new SQLContext(ctx.sc());

        DataFrame df=IBCF_method.getData(ctx,inputFileName);
        DataFrame lines2=df.filter(df.col("buy_cnt").gt(0))
                .select(df.col("user_id"), df.col("sku"), df.col("dt"),df.col("item_name"));
        DataFrame trainSet=lines2.filter(lines2.col("dt").lt("\"20151101\""));//.filter(lines2.col("dt").gt("\"20150930\""));
        DataFrame testSet=lines2.filter(lines2.col("dt").gt("\"20151031\""));

        DataFrame his_user_item=trainSet.select(trainSet.col("user_id"), trainSet.col("sku"));
        DataFrame real_user_item=testSet.select(testSet.col("user_id"), testSet.col("sku"));
        DataFrame item_itemlist=IBCF_method.getData(ctx,IBCFtableName);//"leyou_db.ibcf_result_id_6to10"
        DataFrame candidateSet=IBCF_method.GetCandidateSet(ctx, his_user_item, item_itemlist, topk);
        Double[] evaluation=IBCF_method.GetPrecisionAndRecall(candidateSet, real_user_item);
        IBCF_method.saveToHive(ctx, candidateSet.toJavaRDD(), "user_id:String;itemlist:String", "leyou_db.ibcf_u2i");
        System.out.println("precision: " + evaluation[0] + ";recall: " + evaluation[1]);
    }
}
