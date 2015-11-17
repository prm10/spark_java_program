package gbdt_test;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.GradientBoostedTrees;
import org.apache.spark.mllib.tree.configuration.BoostingStrategy;
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;


public final class GBDTTest {
	public static void main(String[] args) throws Exception{

		SparkConf sparkConf = new SparkConf().setAppName("JavaGradientBoostedTrees");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		// Load and parse data from hive table
		HiveContext hiveCtx = new HiveContext(sc.sc());
		DataFrame user_item_df = hiveCtx.sql("select * from tmalldb.user_item_feat");

		JavaRDD<LabeledPoint> data;
		data = user_item_df.toJavaRDD().map(new Function<Row, LabeledPoint>(){
			public LabeledPoint call (Row row) throws Exception{
				return new LabeledPoint(row.getDouble(10),
						Vectors.dense(row.getLong(2),
								row.getLong(3),
								row.getLong(4),
								row.getLong(5),
								row.getLong(6),
								row.getLong(7),
								row.getLong(8),
								row.getLong(9))
				);
			}
		});
		//String datapath = args[0];
		//JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc.sc(), datapath).toJavaRDD();
		// Split the data into training and test sets (30% held out for testing)
		JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.7, 0.3});
		JavaRDD<LabeledPoint> trainingData = splits[0];
		JavaRDD<LabeledPoint> testData = splits[1];

		// Train a GradientBoostedTrees model.
		//  The defaultParams for Regression use SquaredError by default.
		BoostingStrategy boostingStrategy = BoostingStrategy.defaultParams("Regression");
		boostingStrategy.setNumIterations(3); // Note: Use more iterations in practice.
		boostingStrategy.getTreeStrategy().setMaxDepth(5);
		//  Empty categoricalFeaturesInfo indicates all features are continuous.
		Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
		boostingStrategy.treeStrategy().setCategoricalFeaturesInfo(categoricalFeaturesInfo);

		final GradientBoostedTreesModel model =
			  GradientBoostedTrees.train(trainingData, boostingStrategy);

		//Evaluate model on test instances and compute test error
		JavaPairRDD<Double, Double> predictionAndLabel = 
			testData.mapToPair(new PairFunction<LabeledPoint, Double, Double>(){
					@Override
					    public Tuple2<Double, Double> call(LabeledPoint p) {
					          return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
						  }
					});
		Double testMSE = predictionAndLabel.map(new Function<Tuple2<Double, Double>, Double>() {
					      @Override
					          public Double call(Tuple2<Double, Double> pl) {
						        Double diff = pl._1() - pl._2();
							      return diff * diff;
							          }
					 }).reduce(new Function2<Double, Double, Double>() {
						     @Override
						         public Double call(Double a, Double b) {
							       return a + b;
							           }
					 }) /testData.count();
		System.out.println("Test Mean Squared Error: " + testMSE);
		System.out.println("Learned regression GBT model:\n" + model.toDebugString());
	}
}
