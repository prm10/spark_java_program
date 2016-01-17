package PrefixSpanMethod;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by prm14 on 2016/1/17.
 */
public class PrefixSpan_method {
    public static StructType gernerateStructType(String s) {
        String[] kvs = s.split(";");
        List<StructField> fields = new ArrayList<StructField>();
        for (String x : kvs) {
            String[] kv = x.split(":");
            String name = kv[0];
            String dataType = kv[1];
            if (dataType.equals("String")) {
                fields.add(DataTypes.createStructField(name, DataTypes.StringType, true));
            }
            if (dataType.equals("Long")) {
                fields.add(DataTypes.createStructField(name, DataTypes.LongType, true));
            }
        }
        return DataTypes.createStructType(fields);
    }

    public static JavaRDD<Row> generateRow(JavaPairRDD<String, String> x) {
        return x.map(new Function<Tuple2<String, String>, Row>() {
            @Override
            public Row call(Tuple2<String, String> s) throws Exception {
                return RowFactory.create(s._1, s._2);
            }
        });
    }

    public static void saveToHive(JavaSparkContext ctx, JavaRDD<Row> outfile, String structType, String tableName) {
        HiveContext hiveCtx = new HiveContext(ctx.sc());
        DataFrame result_df = hiveCtx.createDataFrame(outfile, gernerateStructType(structType));
        result_df.printSchema();
        result_df.registerTempTable("temp_table1");
        hiveCtx.sql("drop table if exists " + tableName);
        hiveCtx.sql("create external table if not exists " + tableName + " as select * from temp_table1");
    }
}
